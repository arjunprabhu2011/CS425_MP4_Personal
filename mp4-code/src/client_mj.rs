use std::io::{self, Read, Result};
use std::net::{TcpStream, SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::utils_messages_types::{MessageType, TaskType};
use crate::utils_funcs::get_ip_addr;
use crate::utils_consts::MP4_PORT;

#[derive(Clone)]
pub struct ClientMapleJuice {
    pub machine_domain_name: Arc<String>,
    pub mj_leader_domain_name: Arc<Mutex<String>>,
    pub sdfs_leader_domain_name: Arc<Mutex<String>>
}

/**
 * Main structure of the maplejuice client to handle tasks from leader
 */
impl ClientMapleJuice {
    pub fn new(machine_name: Arc<String>, mj_leader_name: Arc<Mutex<String>>, sdfs_leader_name: Arc<Mutex<String>>) -> Self {
        ClientMapleJuice { 
            machine_domain_name: machine_name,
            mj_leader_domain_name: mj_leader_name,
            sdfs_leader_domain_name: sdfs_leader_name
        } 
    }

    /** 
    * Main function to start functionality for the mj client to handle tasks 
    */
    pub fn start_listening(&mut self) {
        let mut self_clone = self.clone();

        let leader_thread_handle = thread::spawn(move || {
            if let Err(e) = self_clone.listen_for_requests() {
                eprintln!("Error listening to mj leader msgs: {}", e);
            }
        });
    }

    fn listen_for_requests(&mut self) -> Result<()> {
        let ip_addr = get_ip_addr(&self.machine_domain_name);

        match ip_addr {
            Ok(ip) => {
                // TODO: CHANGE PORT
                let address_str = format!("{}:{}", ip.to_string(), MP4_PORT);
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_addr) => {
                        let listener = TcpListener::bind(socket_addr)?;
                        println!("ADD MJ CLIENT LISTENER: {:?}", socket_addr);
                        for stream in listener.incoming() {
                            match stream {
                                Ok(stream) => {
                                    println!("MJ CLIENT GOT MSG");
                                    self.parse_leader_request(stream);
                                },
                                Err(e) => {
                                    eprintln!("Connection failed in MJ client listen_for_requests: {}", e);
                                }
                            }
                        }
                    },
                    Err(e) => {
                        eprintln!("Address parse error in MJ client listen_for_requests: {}", e);
                    }
                }
            }, 
            Err(e) => {
                eprintln!("Get ip address error in MJ client listen_for_requests: {}", e);
            }
        };

        Ok(())
    }

    fn parse_leader_request(&mut self, mut stream: TcpStream) -> io::Result<()> {
        let mut data = Vec::new();
        let mut buffer = [0; 4096];

        while let Ok(bytes_read) = stream.read(&mut buffer) {
            if bytes_read == 0 {
                break;
            }
            data.extend_from_slice(&buffer[..bytes_read]);
        }

        self.execute_request(data)
    }

    fn execute_request(&mut self, data: Vec<u8>) -> io::Result<()> {
        match bincode::deserialize::<MessageType>(&data) {
            Ok(MessageType::MapleHandlerRequestType(maple_handler_request)) => {
                let mut self_clone = self.clone();
                let leader_thread_handle = thread::spawn(move || {
                    self_clone.handle_maple_leader_request(maple_handler_request);
                });
                Ok(())
            },
            Ok(MessageType::JuiceHandlerRequestType(juice_handler_request)) => {
                let mut self_clone = self.clone();
                let leader_thread_handle = thread::spawn(move || {
                    self_clone.handle_juice_leader_request(juice_handler_request);
                });
                Ok(())
            },
            Ok(MessageType::FullTaskCompleteAckType(full_task_complete_ack)) => {
                let mut task_type = "";
                if full_task_complete_ack.task_type == TaskType::Maple {
                    task_type = "MAPLE";
                } else {
                    task_type = "JUICE";
                }

                println!("RECEIVED ACKNOWLEDGEMENT OF {} TASK RUNNING {} EXECUTABLE", task_type, full_task_complete_ack.exe_file);
                Ok(())
            },
            Ok(MessageType::MapleRequestType(_maple_request)) => {
                Ok(())
            }, 
            Ok(MessageType::JuiceRequestType(_juice_request)) => {
                Ok(())
            },
            Ok(MessageType::MapleCompleteAckType(_maple_complete_ack)) => {
                Ok(())
            },
            Ok(MessageType::JuiceCompleteAckType(_juice_complete_ack)) => {
                Ok(())
            },
            Err(e) => {
                println!("ERR: {:?}", e);
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Could not deserialize to any client request or completion ack struct",
                ))
            }
        }
    }
}

