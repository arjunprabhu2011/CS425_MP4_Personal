use std::env;
use std::io::{self};
use std::net::{ToSocketAddrs, UdpSocket, TcpStream, TcpListener, SocketAddr, AddrParseError};
use std::process::exit;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::collections::HashMap;
use rand::seq::SliceRandom;
use serde_cbor::de::from_slice;
use serde_cbor::ser::to_vec;
use rand::Rng;
use serde::{Serialize, Deserialize};
use std::cmp::min;
use std::io::{Read, Write};
use std::fs::File;

//List of all machines in the distributed system. The 80 is just a default port for now so that get_ip_addr does not return an error
const INTRODUCER_NAME: &str = "fa23-cs425-4601.cs.illinois.edu:80";  

const RANDOM_SELECTION_COUNT: usize = 4;
const PORT_8000: usize = 8000;

const SUSPICION_TIMEOUT: u64 = 4;
const FAILURE_TIMEOUT: u64 = 5;  
const CLEANUP_TIMEOUT: u64 = 4;


#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
enum NodeStatus {
    Alive,
    Suspicious,
    Left,
    Crashed,
}

fn main() {
    println!("Starting main");
    // initialize local membership lists, T_fail, T_sus, T_
    // Check if this machine is introducer
    let args: Vec<String> = env::args().collect();

    let mut machine_num: usize = 0;
    let suspicion_active: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
    let drop_rate: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));

    for arg in args[1..].iter() {
        if arg.starts_with("--current-machine=") {
            let value_str = arg.split('=').nth(1).unwrap_or("");
            let current_machine_value: Option<usize> = value_str.parse().ok();

            // Check introducer machine
            if let Some(value) = current_machine_value {
                machine_num = value;
            }
        }
    }

    if machine_num == 0 {
        println!("Must provide current machine argument!");
        exit(1);
    }

    let log_file_name = format!("logs/machine.{}.log", machine_num); 

    let log_file = File::open(log_file_name).expect("Couldn't open file");
    let arc_of_log_file = Arc::new(Mutex::new(log_file));

    let membership_list: Arc<Mutex<HashMap<(String, usize, SystemTime), (u64, u64, NodeStatus)>>> = Arc::new(Mutex::new(HashMap::new()));

    let domain_name = if machine_num < 10 {
        format!("fa23-cs425-460{}.cs.illinois.edu:80", machine_num)
    } else {
        format!("fa23-cs425-46{}.cs.illinois.edu:80", machine_num)
    };

    let mut node_ip_address = String::new();
    match get_ip_addr(&domain_name) {
        Ok(ip) => {
            node_ip_address = ip.to_string();
        },
        Err(_) => {
            eprintln!("Issue with getting ip address");
        }
    }

    let udp_socket = match UdpSocket::bind(format!("{}:{}", node_ip_address, PORT_8000 + machine_num)) {
        Ok(socket) => socket,
        Err(e) => {
            println!("Failed to bind socket LISTENER: {}", e);
            return;
        }
    };

    let udp_socket = Arc::new(Mutex::new(udp_socket));

    node_join(machine_num, &membership_list, &udp_socket);

    let membership_list_arc_clone = Arc::clone(&membership_list);
    let suspicion_bool_clone = Arc::clone(&suspicion_active);
    let log_file_clone = Arc::clone(&arc_of_log_file);
    let drop_rate_clone = Arc::clone(&drop_rate);
    let cli_thread = thread::spawn(move || {
        run_cli(&membership_list_arc_clone, machine_num, &suspicion_bool_clone, &log_file_clone, &drop_rate_clone);
    });

    let membership_list_arc_clone = Arc::clone(&membership_list);
    let udp_socket_clone = Arc::clone(&udp_socket);
    let log_file_clone = Arc::clone(&arc_of_log_file);
    let drop_rate_clone = Arc::clone(&drop_rate);
    let listening_thread = thread::spawn(move || {
        membership_listener(machine_num, &membership_list_arc_clone, &udp_socket_clone, &log_file_clone, &drop_rate_clone);
    });

    let membership_list_arc_clone = Arc::clone(&membership_list);
    let udp_socket_clone = Arc::clone(&udp_socket);
    let log_file_clone = Arc::clone(&arc_of_log_file);
    let sending_thread = thread::spawn(move || {
        membership_sender(&membership_list_arc_clone, &udp_socket_clone, &log_file_clone);
    });

    let membership_list_arc_clone = Arc::clone(&membership_list);
    let suspicion_bool_clone = Arc::clone(&suspicion_active);
    let log_file_clone = Arc::clone(&arc_of_log_file);
    let failure_detection_thread = thread::spawn(move || {
        check_list_for_failure(machine_num, &membership_list_arc_clone, &suspicion_bool_clone, &log_file_clone);
    });

    let suspicion_bool_clone = Arc::clone(&suspicion_active);
    let drop_rate_clone = Arc::clone(&drop_rate);
    let suspicion_listener_thread = thread::spawn(move || {
        if let Err(e) = listen_for_suspicion_and_drop_rate_changes(&suspicion_bool_clone, machine_num, &drop_rate_clone) {
            eprintln!("Error reading from suspicion stream: {}", e);
        }
    });

    // Join threads
    if let Err(e) = cli_thread.join() {
        println!("Failed to join CLI thread: {:?}", e);
    }

    if let Err(e) = listening_thread.join() {
        println!("Failed to join listening thread: {:?}", e);
    }
 
    if let Err(e) = sending_thread.join() {
        println!("Failed to join sending thread: {:?}", e);
    }

    if let Err(e) = failure_detection_thread.join() {
        println!("Failed to join failure detection thread: {:?}", e);
    }

    if let Err(e) = suspicion_listener_thread.join() {
        println!("Failed to join Suspicion Listener thread: {:?}", e);
    }
}


/**
 * Helper method to detect commands '' in the CLI (the terminal)
 * membership_list - membership list 
 * machine_number - the machine number 
 * suspicion_active - is suspicion active 
 * arc_log_file - log file 
 * arc_drop_rate - the drop rate 
 */
fn run_cli(membership_list: &Arc<Mutex<HashMap<(String, usize, SystemTime), (u64, u64, NodeStatus)>>>, machine_number: usize, suspicion_active: &Arc<Mutex<bool>>, arc_log_file: &Arc<Mutex<File>>, arc_drop_rate: &Arc<Mutex<u32>>) {
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
            }
        }
    }   
}

/*
 * Prints the membership list 
 * membership_list - membership list 
 */
fn list_mem(membership_list: &Arc<Mutex<HashMap<(String, usize, SystemTime), (u64, u64, NodeStatus)>>>) {
    if let Ok(locked_membership_list) = membership_list.lock() {
        for (key, value) in &*locked_membership_list {
            println!("{:?} -> {:?}", key, value);
        }

        drop(locked_membership_list);
    } else {
        println!("Failed to acquire lock on membership list");
    }
}

/*
 * Prints the node id 
 * machine_number - machine number 
 * membership_list - membership list 
 */
fn list_self(machine_number: usize, membership_list: &Arc<Mutex<HashMap<(String, usize, SystemTime), (u64, u64, NodeStatus)>>>) {
    if let Ok(locked_membership_list) = membership_list.lock() {
        for key in locked_membership_list.keys() {
            if key.1 == PORT_8000 + machine_number {
                println!("Self ID: {:?}", key);
                return;
            }
        }
        println!("Self not found in membership list");
    } else {
        println!("Failed to acquire lock on membership list");
    }
}

/*
 * Sends a tcp message 
 * machine_number - machine number to send to 
 * message - msg to send 
 * suspicion_active - is suspicion active
 */
fn send_tcp_message(machine_number: usize, message: &str, suspicion_active: &Arc<Mutex<bool>>) {
    let domains: [&str; 10] = [
        "fa23-cs425-4601.cs.illinois.edu:80",
        "fa23-cs425-4602.cs.illinois.edu:80",
        "fa23-cs425-4603.cs.illinois.edu:80",
        "fa23-cs425-4604.cs.illinois.edu:80",
        "fa23-cs425-4605.cs.illinois.edu:80",
        "fa23-cs425-4606.cs.illinois.edu:80",
        "fa23-cs425-4607.cs.illinois.edu:80",
        "fa23-cs425-4608.cs.illinois.edu:80",
        "fa23-cs425-4609.cs.illinois.edu:80",
        "fa23-cs425-4610.cs.illinois.edu:80"
    ];

    for i in 0..domains.len() {
        if i == machine_number - 1 {
            continue;
        }

        let domain_name = domains[i];

        let ip_addr = get_ip_addr(domain_name);

        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:7878", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_address) => {
                        if let Ok(mut stream) = TcpStream::connect(socket_address) {
                            println!("TRIMMED MESSAGE: {}", message.trim());
                            if message.trim() == "enable suspicion" {
                                if let Ok(mut suspicion_bool) = suspicion_active.lock() {
                                    println!("Set suspicion to true");
                                    *suspicion_bool = true;
                                }
                            } else if message.trim() == "disable suspicion" {
                                if let Ok(mut suspicion_bool) = suspicion_active.lock() {
                                    println!("Set suspicion to false");
                                    *suspicion_bool = false;
                                }
                            }
        
                            let _ = stream.write(message.as_bytes());
                        } else {
                            println!("Failed to TCP send message");
                        }
                        println!("Successfully parsed socket address: {:?}", socket_address);
                    },
                    Err(e) => {
                        eprintln!("Failed to parse socket address: {}", e);
                    }
                }
            }, 
            Err(_) => println!("Issue with sending TCP Suspicion bytes")
        }
    }
}

/**
 * If a packet is read on the stream, this method parses it and performs the proper action
 * stream - tcp stream
 * suspicion_active - is suspicion active
 * arc_drop_rate: the drop rate of packets  
 */
fn read_suspicion_and_drop_rate_changes(mut stream: TcpStream, suspicion_active: &Arc<Mutex<bool>>, arc_drop_rate: &Arc<Mutex<u32>>) -> io::Result<()> {
    let mut buf = [0; 1024];
    loop {
        let bytes_read = stream.read(&mut buf)?;
        if bytes_read == 0 {
            return Ok(());
        }

        let message = String::from_utf8_lossy(&buf[..bytes_read]);
        println!("Received: {}", message);

        println!("MESSAGE 1: {}", message);

        if message.trim() == "enable suspicion" {
            if let Ok(mut suspicion_bool) = suspicion_active.lock() {
                *suspicion_bool = true;
            }

            println!("SUSPICION ENABLED!");
        } else if message.trim() == "disable suspicion" {
            if let Ok(mut suspicion_bool) = suspicion_active.lock() {
                *suspicion_bool = false;
            }

            println!("SUSPICION DISABLED!");
        } else if message.starts_with("DROP RATE") {
            let new_drop_rate_str = message.trim().split(' ').nth(2).unwrap_or("");
            let new_drop_rate: Option<u32> = new_drop_rate_str.parse().ok();

            if let Ok(mut drop_rate) = arc_drop_rate.lock() {
                if let Some(dr) = new_drop_rate {
                    println!("NEW VALUE: {}", dr);
                    *drop_rate = dr;
                }
            }
        }
    }
}

/**
 * Listens for suspicion and the changes in the drop rate of packets 
 * suspicion_active - is suspicion active
 * machine num - machine number that is listening 
 * arc_drop_rate: the drop rate of packets  
 */
fn listen_for_suspicion_and_drop_rate_changes(suspicion_active: &Arc<Mutex<bool>>, machine_number: usize, arc_drop_rate: &Arc<Mutex<u32>>) -> io::Result<()> {
    let domains: [&str; 10] = [
        "fa23-cs425-4601.cs.illinois.edu:80",
        "fa23-cs425-4602.cs.illinois.edu:80",
        "fa23-cs425-4603.cs.illinois.edu:80",
        "fa23-cs425-4604.cs.illinois.edu:80",
        "fa23-cs425-4605.cs.illinois.edu:80",
        "fa23-cs425-4606.cs.illinois.edu:80",
        "fa23-cs425-4607.cs.illinois.edu:80",
        "fa23-cs425-4608.cs.illinois.edu:80",
        "fa23-cs425-4609.cs.illinois.edu:80",
        "fa23-cs425-4610.cs.illinois.edu:80"
    ];

    let ip_addr = get_ip_addr(domains[machine_number - 1]);

    match ip_addr {
        Ok(ip) => {
            let address_str = format!("{}:7878", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_addr) => {
                        let listener = TcpListener::bind(socket_addr)?;

                        for stream in listener.incoming() {
                            match stream {
                                Ok(stream) => {
                                    if let Err(e) = read_suspicion_and_drop_rate_changes(stream, suspicion_active, arc_drop_rate) {
                                        eprintln!("Error: {}", e);
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Connection failed: {}", e);
                                }
                            }
                        }
                    },
                    Err(_) => println!("Couldn't generate socket address")
                }
        }, 
        Err(_) => println!("Couldn't parse IP address in suspicion listener"),
    };

    Ok(())
}

/**
 * Listens for membership lists from other nodes 
 * machine num - machine number that is listening 
 * membership_list: the membership list
 * udp_socket: socket to send from 
 * arc_log_file: log file
 * listener_drop_rate: the drop rate of packets recieved 
 */
fn membership_listener(machine_num: usize, membership_list: &Arc<Mutex<HashMap<(String, usize, SystemTime), (u64, u64, NodeStatus)>>>, udp_socket: &Arc<Mutex<UdpSocket>>, arc_log_file: &Arc<Mutex<File>>, listener_drop_rate: &Arc<Mutex<u32>>) {
    loop {
        let mut buf = [0; 1024];
        let mut received_list: Option<HashMap<(String, usize, SystemTime), (u64, u64, NodeStatus)>> = None;

        if let Ok(locked_udp_socket) = udp_socket.lock() {
            let _ = locked_udp_socket.set_read_timeout(Some(Duration::from_secs(2)));
            match locked_udp_socket.recv_from(&mut buf) {
                Ok((amt, _src)) => {
                    match from_slice::<HashMap<(String, usize, SystemTime), (u64, u64, NodeStatus)>>(&buf[0..amt]) {
                        Ok(decoded_list) => {
                            // Simulate packet drop for Report. When running normally, DROP_PROB = 0
                            let mut rng = rand::thread_rng();
                            if let Ok(dr) = listener_drop_rate.lock() {
                                if rng.gen_range(0..100) < *dr {
                                    // Sleep for a while so that the socket is not overburdened
                                    thread::sleep(Duration::from_millis(250));
                                    continue;
                                } 
                            }
        
                            received_list = Some(decoded_list);  // update received_list
                        },
                        Err(e) => {
                            println!("Failed to deserialize JSON: {}", e);
                        }
                    }
                },
                Err(_) => {
                    println!("");
                }
            }
        } 

        // Check if we have a valid received_list before calling merge_membership_lists
        if let Some(received_list) = received_list {
            merge_membership_lists(&received_list, membership_list, &arc_log_file, machine_num);
        }

        // Sleep for a while so that the socket is not overburdened
        thread::sleep(Duration::from_millis(250));
    }
}


/*
 * Sends membership list out
 * membership_list: the membership list
 * udp_socket: socket to send from 
 * arc_log_file: log file
 */
fn membership_sender(membership_list: &Arc<Mutex<HashMap<(String, usize, SystemTime), (u64, u64, NodeStatus)>>>, udp_socket: &Arc<Mutex<UdpSocket>>, arc_log_file: &Arc<Mutex<File>>) {
    loop {
        if let Ok(locked_membership_list) = membership_list.lock() {
            let mut key_list: Vec<(String, usize, SystemTime)> = locked_membership_list
                .iter() 
                .filter(|&(_, value)| value.2 == NodeStatus::Alive) 
                .map(|(key, _)| key.clone()) 
                .collect();

            let mut rng = rand::thread_rng();

            key_list.shuffle(&mut rng);

            drop(locked_membership_list);
            
            let mut local_port = 0;
            if let Ok(locked_udp_socket) = udp_socket.lock() {
                local_port = match locked_udp_socket.local_addr() {
                    Ok(addr) => addr.port(),
                    Err(e) => {
                        println!("Failed to get local address: {}", e);
                        continue;
                    }
                };

                drop(locked_udp_socket);
            }

            for i in 0..min(RANDOM_SELECTION_COUNT, key_list.len()) {
                if key_list[i].1 != (local_port as usize) {
                    let ip = &key_list[i].0;
                    let port = key_list[i].1;

                    let address_to_send_to: Result<SocketAddr, AddrParseError> = format!("{}:{}", ip, port).parse();

                    if let Ok(mut locked_membership_list) = membership_list.lock() {
                        match address_to_send_to {
                            Ok(addr) => {
                                if let Ok(locked_udp_socket) = udp_socket.lock() {
                                    match locked_udp_socket.local_addr() {
                                        Ok(addr) => {
                                            let ip = addr.ip().to_string();
                                            let port = addr.port();
                                            let mut key_of_local: Option<(String, usize, SystemTime)> = None;
                        
                                            for key in locked_membership_list.keys() {
                                                if key.0 == ip && key.1 == (port as usize) {
                                                    key_of_local = Some(key.clone());
                                                    break;
                                                }
                                            }
                        
                                            if let Some(key) = key_of_local {
                                                if let Some(val) = locked_membership_list.get_mut(&key) {
                                                    val.0 += 1;
    
                                                    let current_time = SystemTime::now();
                                                    if let Ok(duration) = current_time.duration_since(UNIX_EPOCH) {
                                                        let seconds = duration.as_secs();
                                                        val.1 = seconds;
                                                    }
                                                } else {
                                                    println!("Couldn't find key when updating heartbeat");
                                                }
                                            }
                                        }, 
                                        Err(_) => {
                                            println!("Error finding address of UDP socket when updating heartbeat");
                                        }
                                    }
    
                                    let cbor_of_membership_list = match to_vec(&*locked_membership_list) {
                                        Ok(cbor) => cbor,
                                        Err(e) => {
                                            println!("Failed to serialize membership list to CBOR: {}", e);
                                            thread::sleep(Duration::from_secs(2));
                                            continue;
                                        }
                                    };
    
                                    if let Err(e) = locked_udp_socket.send_to(&cbor_of_membership_list, &addr) {
                                        println!("Failed to send membership list to {}: {}", addr, e);
                                    }
    
                                    drop(locked_udp_socket);
                                }
                            },
                            Err(e) => {
                                println!("Failed to parse address: {}", e);
                            }
                        }

                        drop(locked_membership_list);
                    }
                }
            }  
        }
        // Sleep before the next round so that you only send heartbeats every two seconds
        thread::sleep(Duration::from_secs(1));
    }
}


/**
 * Merges two membership lists 
 * @membership_list_one     The membership list which has just been received that will be merged into local mem list
 * @membership_list_two     The local membership list which is being updated
 */
fn merge_membership_lists(membership_list_one: &HashMap<(String, usize, SystemTime), (u64, u64, NodeStatus)>, membership_list_two: &Arc<Mutex<HashMap<(String, usize, SystemTime), (u64, u64, NodeStatus)>>>, arc_log_file: &Arc<Mutex<File>>, machine_num: usize) {
    if let Ok(mut locked_mem_list_two) = membership_list_two.lock() {
        let current_time = SystemTime::now();
        if let Ok(duration) = current_time.duration_since(UNIX_EPOCH) {
            let seconds = duration.as_secs();

            // Loop through membership list and check with other one 
            for (key, value1) in membership_list_one {
                if let Some(value2) = locked_mem_list_two.get(key) {
                    if value1.0 > value2.0 {
                        locked_mem_list_two.insert(key.clone(), (value1.0, seconds, value1.2.clone()));
                    }
                } else {
                    if value1.2 == NodeStatus::Alive || value1.2 == NodeStatus::Suspicious {
                        locked_mem_list_two.insert(key.clone(), (value1.0, seconds, value1.2.clone()));
                    }
                }
            }
        } else {
            println!("Issue with finding duration since start time");
        }

        drop(locked_mem_list_two);
    } else {
        println!("Issue locking the local membership list");
    }
}

/**
 * Checks the membership lists for failure/suspicion
 * machine number - the machine number leaving 
 * mem_list - the membership list
 * suspicion_active - is the suspicion mechanism active 
 * arc_log_file - log file 
 */
fn check_list_for_failure(machine_number: usize, membership_list: &Arc<Mutex<HashMap<(String, usize, SystemTime), (u64, u64, NodeStatus)>>>, suspicion_active: &Arc<Mutex<bool>>, arc_log_file: &Arc<Mutex<File>>) {
    loop {
        if let Ok(mut locked_membership_list) = membership_list.lock() {
            let current_time = SystemTime::now();
            
            let duration = match current_time.duration_since(UNIX_EPOCH) {
                Ok(dur) => dur,
                Err(_) => {
                    println!("Issue with duration");
                    continue;
                },
            };

            let mut nodes_to_mark_as_failed: Vec<((String, usize, SystemTime), (u64, u64, NodeStatus))> = Vec::new();
            let mut nodes_to_delete: Vec<(String, usize, SystemTime)> = Vec::new();

            for key in locked_membership_list.keys() {
                let port = PORT_8000 + machine_number;
                if key.1 == port {
                    continue;
                }

                match locked_membership_list.get(key) {
                    Some(value) => {
                        let seconds = duration.as_secs();
                        if value.2 == NodeStatus::Alive && seconds - value.1 >= FAILURE_TIMEOUT {
                            if let Ok(suspicion_bool) = suspicion_active.lock() {
                                if *suspicion_bool == true {
                                    nodes_to_mark_as_failed.push((key.clone(), (value.0, value.1, NodeStatus::Suspicious)));
                                    //PUT IN TIME FOR NODE AS CRASHED
                                    println!("Node {:?} marked as Suspicious at time {} seconds since Epoch", key, seconds);
                                } else {
                                    nodes_to_mark_as_failed.push((key.clone(), (value.0, value.1, NodeStatus::Crashed)));
                                    println!("Node {:?} marked as Crashed at time {} seconds since Epoch", key, seconds);
                                }
                            }
                        }

                        if value.2 == NodeStatus::Suspicious && seconds - value.1 >= FAILURE_TIMEOUT + SUSPICION_TIMEOUT {
                            nodes_to_mark_as_failed.push((key.clone(), (value.0, value.1, NodeStatus::Crashed)));
                            println!("Node {:?} marked as Crashed at time {} seconds since Epoch", key, seconds);
                        }

                        if let Ok(suspicion_bool) = suspicion_active.lock() {
                            if *suspicion_bool == true {
                                if (value.2 == NodeStatus::Crashed) && (seconds - value.1 >= FAILURE_TIMEOUT + SUSPICION_TIMEOUT + CLEANUP_TIMEOUT) {
                                    nodes_to_delete.push(key.clone());
                                }
                            } else {
                                if (value.2 == NodeStatus::Crashed) && (seconds - value.1 >= FAILURE_TIMEOUT + CLEANUP_TIMEOUT) {
                                    nodes_to_delete.push(key.clone());
                                }
                            }
                        }

                        if (value.2 == NodeStatus::Left) && (seconds - value.1 >= CLEANUP_TIMEOUT) {
                            println!("Node {:?} Left the Network at time at time {} seconds since Epoch", key, seconds);

                            nodes_to_delete.push(key.clone());
                        }
                    }, 
                    None => {
                        println!("Error with getting key from membership list");
                    }
                }
            }

            for (key, value) in nodes_to_mark_as_failed {
                locked_membership_list.insert(key, value);
            }

            for key in nodes_to_delete {
                match locked_membership_list.remove(&key) {
                    Some(value) => {
                        println!("Removed Entry for node {:?} with value {:?}", key, value);
                    },
                    None => println!("No value found for key {:?}", key),
                }
            }

            drop(locked_membership_list);
        }

        thread::sleep(Duration::from_millis(500));
    }
}

/* 
* A node is leaving the network 
* machine number - the machine number leaving 
* mem_list - the membership list
*/
fn handle_node_leave(machine_number: usize, membership_list: &Arc<Mutex<HashMap<(String, usize, SystemTime), (u64, u64, NodeStatus)>>>) {
    if let Ok(mut locked_membership_list) = membership_list.lock() {
        let port = PORT_8000 + machine_number;

        for (key, value) in locked_membership_list.iter_mut() {
            if key.1 == port {
                *value = (value.0, value.1, NodeStatus::Left);
                break;
            }
        }
    }

    thread::sleep(Duration::from_secs(2));
    exit(0);
}

/* 
* A node is joining the network 
* machine number - the machine number joinning 
* mem_list - the membership list
* udp_socket - the socket joining 
*/
fn node_join(machine_number: usize, mem_list: &Arc<Mutex<HashMap<(String, usize, SystemTime), (u64, u64, NodeStatus)>>>, udp_socket: &Arc<Mutex<UdpSocket>>) {
    println!("MACHINE {} JOINED", machine_number);
    if machine_number == 1 {
        // Introducer joinning 
        let domain_name = INTRODUCER_NAME;

        let mut node_ip_address = String::new();

        match get_ip_addr(&domain_name) {
            Ok(ip) => {
                node_ip_address = ip.to_string();
            },
            Err(_) => {
                eprintln!("Issue with getting ip address");
            }
        }

        let join_timestamp = SystemTime::now();
        let last_heartbeat = 0;
        let last_heartbeat_seconds = 0;
        let status: NodeStatus = NodeStatus::Alive;

        if let Ok(mut locked_mem_list) = mem_list.lock() {
            locked_mem_list.insert((node_ip_address, PORT_8000 + machine_number, join_timestamp), (last_heartbeat, last_heartbeat_seconds, status));
        }
    } else {
        let domain_name = if machine_number < 10 {
            format!("fa23-cs425-460{}.cs.illinois.edu:80", machine_number)
        } else {
            format!("fa23-cs425-46{}.cs.illinois.edu:80", machine_number)
        };

        let mut node_ip_address = String::new();
        match get_ip_addr(&domain_name) {
            Ok(ip) => {
                node_ip_address = ip.to_string();
            },
            Err(_) => {
                eprintln!("Issue with getting ip address");
            }
        }

        // Add node to network 
        let join_timestamp = SystemTime::now();
        let last_heartbeat = 0;
        let last_heartbeat_seconds = 0;
        let status: NodeStatus = NodeStatus::Alive;
        
        if let Ok(mut locked_mem_list) = mem_list.lock() {
            locked_mem_list.insert((node_ip_address, PORT_8000 + machine_number, join_timestamp), (last_heartbeat, last_heartbeat_seconds, status));
        }

        let introducer_ip_address = get_ip_addr(&INTRODUCER_NAME);
        match introducer_ip_address {
            Ok(ip) => {
                let introducer_full_address = format!("{}:{}", ip, PORT_8000 + 1);

                // Convert membership list to binary
                if let Ok(locked_mem_list) = mem_list.lock() {
                    let cbor_of_membership_list = match to_vec(&*locked_mem_list) {
                        Ok(cbor) => cbor,
                        Err(e) => {
                            println!("Failed to serialize membership list to CBOR: {}", e);
                            exit(0);
                        }
                    };
                    
                    if let Ok(locked_udp_socket) = udp_socket.lock() {
                        if let Err(e) = locked_udp_socket.send_to(&cbor_of_membership_list, &introducer_full_address) {
                            println!("Failed to send membership list to introducer: {}", e);
                        }
                    }
                }
            }, 
            Err(e) => {
                println!("Failed to parse address: {}", e);
            }
        }
    }
}

/**
 * Get the ip address based on a domain
 */
fn get_ip_addr(domain: &str) -> Result<Arc<String>, String> {
    match domain.to_socket_addrs() {
        Ok(mut addrs) => {
            if let Some(addr) = addrs.next() {
                let current_ip_address = Arc::new(format!("{}", addr.ip()));
                println!("Domain: {} , IP address: {}", domain, current_ip_address);
                Ok(current_ip_address)
            } else {
                Err(String::from("No IP addresses found for the given domain."))
            }
        },
        Err(e) => {
            Err(format!("Could not resolve the domain: {}", e))
        }
    }
}