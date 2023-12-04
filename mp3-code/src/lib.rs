use std::io::{Write, self, Read};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{SystemTime, Instant};
use std::collections::HashMap;
use std::fs::File;
use std::io::Result;


use std::env;
use std::net::{ToSocketAddrs, UdpSocket, TcpStream, TcpListener, SocketAddr, AddrParseError};
use std::process::exit;
use std::time::{Duration, UNIX_EPOCH};
use rand::seq::SliceRandom;
use serde_cbor::de::from_slice;
use serde_cbor::ser::to_vec;
use rand::Rng;
use serde::{Serialize, Deserialize};
use std::cmp::min;

pub mod mp2; 
pub mod leader; 
pub mod replica;
pub mod utils;
pub mod client;

pub use utils::ResponseWrapper;

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub enum NodeStatus {
    Alive,
    Suspicious,
    Left,
    Crashed,
}

pub use mp2::{
    list_mem,
    list_self,
    handle_node_leave,
    send_tcp_message
};

pub use utils::{
    WriteRequest,
    RequestType,
    ClientRequest,
    ReadRequest,
    DeleteRequest,
    LsRequest,
    DOMAINS,
    get_ip_addr,
};

/**
 * Helper method to detect commands '' in the CLI (the terminal)
 * membership_list - membership list 
 * machine_number - the machine number 
 * suspicion_active - is suspicion active 
 * arc_drop_rate - the drop rate 
 */
pub fn run_cli(membership_list: &Arc<Mutex<HashMap<(String, usize, SystemTime), (u64, u64, NodeStatus)>>>, machine_number: usize, suspicion_active: &Arc<Mutex<bool>>, arc_drop_rate: &Arc<Mutex<u32>>, machine_domain_name: Arc<String>, leader_domain_name: Arc<Mutex<String>>, files: Arc<Mutex<Vec<(String, u16)>>>) {
    loop {
        let mut input = String::new();
        
        if io::stdin().read_line(&mut input).is_ok() {
            let input = input.trim();

            if input.starts_with("list_mem") {
                list_mem(membership_list);
            } else if input.starts_with("list_self") {
                list_self(machine_number, membership_list);
            } else if input.starts_with("leave") {
                handle_node_leave(machine_number, membership_list);
            } else if input.starts_with("enable suspicion") {
                send_tcp_message(machine_number, "enable suspicion", suspicion_active);
            } else if input.starts_with("disable suspicion") {
                send_tcp_message(machine_number, "disable suspicion", suspicion_active);
            } else if input.starts_with("introduce_droprate") {
                println!("INTRODUCE DROPRATE");
                let new_drop_rate_str = input.split(' ').nth(1).unwrap_or("");
                let new_drop_rate: Option<u32> = new_drop_rate_str.parse().ok();

                if let Ok(mut drop_rate) = arc_drop_rate.lock() {
                    if let Some(dr) = new_drop_rate {
                        println!("NEW VALUE: {}", dr);
                        *drop_rate = dr;
                    }

                    send_tcp_message(machine_number, &format!("DROP RATE: {}", *drop_rate), suspicion_active);
                }
            } else if input.starts_with("get") {
                let sdfs_file = input.split(' ').nth(1).unwrap_or("");
                let local_file = input.split(' ').nth(2).unwrap_or("");
                
                handle_get(sdfs_file.to_string(), local_file.to_string(), machine_domain_name.to_string(), leader_domain_name.clone());
            } else if input.starts_with("put") {
                let sdfs_file = input.split(' ').nth(2).unwrap_or("");
                let local_file = input.split(' ').nth(1).unwrap_or("");
                println!("ENTERRED PUT");
                handle_put(sdfs_file.to_string(), local_file.to_string(), machine_domain_name.to_string(), leader_domain_name.clone())
            } else if input.starts_with("delete") {
                let sdfs_file = input.split(' ').nth(1).unwrap_or("");

                handle_delete(sdfs_file.to_string(), machine_domain_name.to_string(), leader_domain_name.clone());
            } else if input.starts_with("ls") {
                let sdfs_file = input.split(' ').nth(1).unwrap_or("");

                handle_ls(sdfs_file.to_string(), machine_domain_name.to_string(), leader_domain_name.clone());
            } else if input.starts_with("store") {
                print_current_files(files.clone());
            } else if input.starts_with("multiread") {
                let args: Vec<&str> = input.split_whitespace().collect();

                handle_multiread(&args, leader_domain_name.clone());
            }
        }
    }   
}

pub fn print_current_files(current_files: Arc<Mutex<Vec<(String, u16)>>>) {
    if let Ok(files) = current_files.lock() {
        for (filename, shard) in files.iter() {
            println!("Filename: {}, Shard: {}", filename, shard);
        }
    }
}

/**
 * Initial Handling of put request
 */
pub fn handle_put(sdfs_file: String, unix_file: String, machine_domain_name: String, leader_domain_name: Arc<Mutex<String>>) {
    let write_req = WriteRequest {
        sdfs_filename: sdfs_file,
        unix_filename: unix_file,
        data: Vec::new(),
        requesting_machine_domain_name: machine_domain_name,
        shard_number: 6,
        request_type: RequestType::Write
    };

    println!("HANDLE PUT SEND PUT REQUEST: {:?}", &write_req);
    send_put_request_to_leader(write_req, leader_domain_name);
}

/**
 * Sends the put request to the leader in order to obtain replicas
 */
pub fn send_put_request_to_leader(req: WriteRequest, leader_domain_name: Arc<Mutex<String>>) -> Result<()> {
    println!("SEND PUT REQUEST TO LEADER SEND PUT REQUEST: {:?}", &req);
    if let Ok(leader_name) = leader_domain_name.lock() {
        let ip_addr = get_ip_addr(&leader_name);

        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:8017", ip.to_string());
                println!("ADD {}", address_str);
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_address) => {
                        if let Ok(mut stream) = TcpStream::connect(socket_address) {
                            let client_request = ClientRequest::Write(req);
                            let serialized_request = bincode::serialize(&client_request);

                            match serialized_request {
                                Ok(ser_req) => {
                                    println!("SENT PUT REQUEST TO LEADER");
                                    stream.write(&ser_req);
                                },
                                Err(_) => {
                                    eprintln!("Error in sending serialized response in send_shard_to_other_replica");
                                }
                            }
                        } else {
                            println!("Failed to TCP send message SEND PUT REQUEST TO LEADER SDFS");
                        }
                    },
                    Err(_) => {
                        eprintln!("Issue with parsing address in send_shard_to_other_replica");
                    }
                }
            },
            Err(_) => {
                eprintln!("Issue with getting ip address in send_shard_to_other_replica");
            }
        }
    }

    Ok(())
}

/**
 * Initial Handling of get request 
 */
pub fn handle_get(sdfs_file: String, unix_file: String, machine_domain_name: String, leader_domain_name: Arc<Mutex<String>>) {
    let read_req = ReadRequest {
        sdfs_filename: sdfs_file,
        unix_filename: unix_file,
        requesting_machine_domain_name: machine_domain_name,
        shard_number: 6,
        request_type: RequestType::Read
    };

    println!("HANDLE GET SEND REQUEST: {:?}", &read_req);

    send_get_request_to_leader(read_req, leader_domain_name);
}

/**
 * Sends get request to leader to obtain replicas
 */
pub fn send_get_request_to_leader(req: ReadRequest, leader_domain_name: Arc<Mutex<String>>) -> Result<()> {
    println!("SEND GET REQUEST TO LEADER SEND REQUEST: {:?}", &req);
    if let Ok(leader_name) = leader_domain_name.lock() {
        let ip_addr = get_ip_addr(&leader_name);
        

        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:8017", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_address) => {
                        if let Ok(mut stream) = TcpStream::connect(socket_address) {
                            let client_request = ClientRequest::Read(req);
                            let serialized_request = bincode::serialize(&client_request);

                            match serialized_request {
                                Ok(ser_req) => {
                                    println!("SENT GET REQUEST TO LEADER");
                                    stream.write(&ser_req);
                                },
                                Err(_) => {
                                    eprintln!("Error in sending serialized response in send_shard_to_other_replica");
                                }
                            }
                        } else {
                            println!("Failed to TCP send message SEND GET REQUEST TO LEADER SDFS");
                        }
                    },
                    Err(_) => {
                        eprintln!("Issue with parsing address in send_shard_to_other_replica");
                    }
                }
            },
            Err(_) => {
                eprintln!("Issue with getting ip address in send_shard_to_other_replica");
            }
        }
    }

    Ok(())
}

/**
 * Initial handling of delete request 
 */
pub fn handle_delete(sdfs_file: String, machine_domain_name: String, leader_domain_name: Arc<Mutex<String>>) {
    let delete_req = DeleteRequest {
        filename: sdfs_file,
        requesting_machine_domain_name: machine_domain_name,
        shard_number: 6,
        request_type: RequestType::Delete
    };

    send_delete_request_to_leader(delete_req, leader_domain_name);
}

/**
 * Sends the delete request to the leader 
 */
pub fn send_delete_request_to_leader(req: DeleteRequest, leader_domain_name: Arc<Mutex<String>>) -> Result<()> {
    if let Ok(leader_name) = leader_domain_name.lock() {
        let ip_addr = get_ip_addr(&leader_name);

        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:8017", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_address) => {
                        if let Ok(mut stream) = TcpStream::connect(socket_address) {
                            let client_request = ClientRequest::Delete(req);
                            let serialized_request = bincode::serialize(&client_request);

                            match serialized_request {
                                Ok(ser_req) => {
                                    println!("SENT DELETE REQUEST TO LEADER");
                                    stream.write(&ser_req);
                                },
                                Err(_) => {
                                    eprintln!("Error in sending serialized response in send_shard_to_other_replica");
                                }
                            }
                        } else {
                            println!("Failed to TCP send message SEND DELETE REQUEST TO LEADER SDFS");
                        }
                    },
                    Err(_) => {
                        eprintln!("Issue with parsing address in send_shard_to_other_replica");
                    }
                }
            },
            Err(_) => {
                eprintln!("Issue with getting ip address in send_shard_to_other_replica");
            }
        }
    }

    Ok(())
}

/**
 * Initial handling of ls command 
 */
pub fn handle_ls(sdfs_file: String, machine_domain_name: String, leader_domain_name: Arc<Mutex<String>>) {
    let ls_req = LsRequest {
        filename: sdfs_file,
        requesting_machine_domain_name: machine_domain_name.to_string()
    };

    send_ls_request_to_leader(ls_req, machine_domain_name, leader_domain_name);
}

/**
 * Sends ls request to leader 
 */
pub fn send_ls_request_to_leader(req: LsRequest, machine_domain_name: String, leader_domain_name: Arc<Mutex<String>>) -> Result<()> {
    if let Ok(leader_name) = leader_domain_name.lock() {
        let ip_addr = get_ip_addr(&leader_name);

        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:8020", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_address) => {
                        if let Ok(mut stream) = TcpStream::connect(socket_address) {
                            let serialized_request = bincode::serialize(&req);

                            match serialized_request {
                                Ok(ser_req) => {
                                    println!("SENT LS REQUEST TO LEADER");
                                    stream.write(&ser_req);
                                },
                                Err(_) => {
                                    eprintln!("Error in sending serialized response in send_shard_to_other_replica");
                                }
                            }
                        } else {
                            println!("Failed to TCP send message SEND LS REQUEST TO LEADER");
                        }
                    },
                    Err(_) => {
                        eprintln!("Issue with parsing address in send_shard_to_other_replica");
                    }
                }
            },
            Err(_) => {
                eprintln!("Issue with getting ip address in send_shard_to_other_replica");
            }
        }
    }

    Ok(())
}

/**
 * Initial handling of multiread 
 */
pub fn handle_multiread(args: &[&str], leader_domain_name:Arc<Mutex<String>>) {
    for &arg in &args[3..] {
        if let Ok(num) = arg[2..].parse::<usize>() {
            let read_req = ReadRequest {
                sdfs_filename: args[1].to_string(),
                unix_filename: args[2].to_string(),
                requesting_machine_domain_name: DOMAINS[num - 1].to_string(),
                shard_number: 6,
                request_type: RequestType::Read
            };

            send_get_request_to_leader(read_req, leader_domain_name.clone());
        }
    }
}

pub fn wait_for_get_ack(machine_domain_name: &str) -> Result<bool> {
    let ip_addr = get_ip_addr(machine_domain_name);

    match ip_addr {
        Ok(ip) => {
            let address_str = format!("{}:8025", ip.to_string());
            match address_str.parse::<SocketAddr>() {
                Ok(socket_addr) => {
                    let listener = TcpListener::bind(socket_addr)?;
                    listener.set_nonblocking(true)?;

                    let start = Instant::now();
                    let timeout = Duration::from_secs(60);

                    loop {
                        match listener.accept() {
                            Ok((stream, _addr)) => {
                                match is_get_ack(&stream) {
                                    Ok(true) => {
                                        println!("RECEIVED GET WAITING ACK!");
                                        return Ok(true);
                                    },
                                    _ => {
                                        continue;
                                    }
                                }
                            }
                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                                // Check if the timeout has elapsed
                                if start.elapsed() >= timeout {
                                    return Err(io::Error::new(io::ErrorKind::TimedOut, "Timeout waiting for ACK"));
                                }
                                
                                // Sleep briefly to avoid busy looping
                                std::thread::sleep(Duration::from_millis(100));
                                continue;
                            }
                            Err(e) => return Err(e),
                        }
                    }
                },
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Issue in parsing address in wait for ack",
                    ));
                }
            }
        }, 
        Err(_) => {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Get IP ADDRESS ERROR IN WAIT FOR ACK",
            ));
        }
    }
}

pub fn wait_for_put_ack(machine_domain_name: &str) -> Result<bool> {
    let ip_addr = get_ip_addr(machine_domain_name);

    match ip_addr {
        Ok(ip) => {
            let address_str = format!("{}:8025", ip.to_string());
            match address_str.parse::<SocketAddr>() {
                Ok(socket_addr) => {
                    let listener = TcpListener::bind(socket_addr)?;
                    listener.set_nonblocking(true)?;

                    let start = Instant::now();
                    let timeout = Duration::from_secs(60);

                    loop {
                        match listener.accept() {
                            Ok((stream, _addr)) => {
                                match is_put_ack(&stream) {
                                    Ok(true) => {
                                        println!("RECEIVED PUT WAITING ACK!");
                                        return Ok(true);
                                    },
                                    _ => {
                                        continue;
                                    }
                                }
                            }
                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                                // Check if the timeout has elapsed
                                if start.elapsed() >= timeout {
                                    return Err(io::Error::new(io::ErrorKind::TimedOut, "Timeout waiting for ACK"));
                                }
                                
                                // Sleep briefly to avoid busy looping
                                std::thread::sleep(Duration::from_millis(100));
                                continue;
                            }
                            Err(e) => return Err(e),
                        }
                    }
                },
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Issue in parsing address in wait for ack",
                    ));
                }
            }
        }, 
        Err(_) => {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Get IP ADDRESS ERROR IN WAIT FOR ACK",
            ));
        }
    }
}

fn is_get_ack(mut stream: &TcpStream) -> Result<bool> {
    let mut data = Vec::new();
    let mut buffer = [0; 4096];

    loop {
        let bytes_read = stream.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }

        data.extend_from_slice(&buffer[..bytes_read]);
    }

    match bincode::deserialize::<ResponseWrapper>(&data) {
        Ok(ResponseWrapper::Read(_value)) => {
            return Ok(true);
        },
        Ok(ResponseWrapper::Write(_value)) => {
            return Ok(false);
        },
        Ok(ResponseWrapper::Delete(_value)) => {
            return Ok(false);
        },
        Err(_) => {
            return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Could not deserialize to any response struct",
                ));
        },
    }
}

fn is_put_ack(mut stream: &TcpStream) -> Result<bool> {
    let mut data = Vec::new();
    let mut buffer = [0; 4096];

    loop {
        let bytes_read = stream.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }

        data.extend_from_slice(&buffer[..bytes_read]);
    }

    match bincode::deserialize::<ResponseWrapper>(&data) {
        Ok(ResponseWrapper::Read(_value)) => {
            return Ok(false);
        },
        Ok(ResponseWrapper::Write(_value)) => {
            return Ok(true);
        },
        Ok(ResponseWrapper::Delete(_value)) => {
            return Ok(false);
        },
        Err(_) => {
            return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Could not deserialize to any response struct",
                ));
        },
    }
}


