use std::collections::{HashMap, HashSet};
use std::io::{self, Read};
use std::net::{TcpStream, SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::SystemTime;
use std::io::Result;
use rand::thread_rng;
use rand::seq::SliceRandom;

use mp3_code::NodeStatus;

use crate::utils_consts::MP4_PORT_LEADER;
use crate::utils_consts::MP4_PORT_CLIENT;
use crate::utils_funcs::{get_ip_addr, send_serialized_message};
use crate::utils_messages_types::{MJTask, MessageType};


#[derive(Clone)]
pub struct LeaderMapleJuice {
    pub machine_domain_name: Arc<String>,
    pub sdfs_leader_domain_name: Arc<Mutex<String>>,
    pub membership_list: Arc<Mutex<HashMap<(String, usize, SystemTime), (u64, u64, NodeStatus)>>>, // (ip, port, timestamp), (last HB, sec since HB, node status)
    pub current_maplejuice_task: Arc<Mutex<Option<MJTask>>>, //Just represent the current maplejuice task since we only need to handle one at a time
    pub sdfs_prefix_to_keys: Arc<Mutex<HashMap<String, Vec<String>>>>, // sdfs prefix to all keys appended to prefix
    pub machine_to_task_map: Arc<Mutex<HashMap<String, Vec<MessageType>>>>, //Represent tasks as message types so it can take in MapleHandlerRequest and JuiceHandlerRequest
    pub key_to_all_values_map: Arc<Mutex<HashMap<String, Vec<Vec<u8>>>>> //Used to collect all values for each key when creating intermediary file and putting it into sdfs
    //machine_to_task_map has a key which is the IP address of the machine, NOT the domain name
}

/**
 * Main structure of the Leader to listen and manage the sdfs
 */
impl LeaderMapleJuice {
    pub fn new(machine_name: Arc<String>, sdfs_leader_name: Arc<Mutex<String>>, mem_list: Arc<Mutex<HashMap<(String, usize, SystemTime), (u64, u64, NodeStatus)>>>) -> Self {
        LeaderMapleJuice { 
            machine_domain_name: machine_name,
            sdfs_leader_domain_name: sdfs_leader_name,
            membership_list: mem_list,
            current_maplejuice_task: Arc::new(Mutex::new(None)),
            sdfs_prefix_to_keys: Arc::new(Mutex::new(HashMap::new())),
            machine_to_task_map: Arc::new(Mutex::new(HashMap::new())),
            key_to_all_values_map: Arc::new(Mutex::new(HashMap::new()))
        } 
    }

    /**
     * Main function to start functionality for the mj Listener 
     */
    pub fn start_listening(&mut self) {
        let mut self_clone = self.clone();

        let clients_thread_handle = thread::spawn(move || {
            if let Err(e) = self_clone.listen_for_requests() {
                eprintln!("Error listening to client requests: {}", e);
            }
        });
    }

    fn listen_for_requests(&mut self) -> Result<()> {
        let ip_addr = get_ip_addr(&self.machine_domain_name);
        println!("LEADER SELF MACHINE DOMAIN NAME: {}", self.machine_domain_name);

        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:{}", ip.to_string(), MP4_PORT_LEADER);
                println!("LEADER LISTEN FOR REQUESTS ADDRESS: {}", address_str);
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_addr) => {
                        println!("RIGHT BEFORREEEE");
                        let listener = TcpListener::bind(socket_addr)?;
                        println!("ADD MJ LEADER LISTENER: {:?}", socket_addr);
                        for stream in listener.incoming() {
                            match stream {
                                Ok(stream) => {
                                    println!("MJ LEADER GOT MSG");
                                    self.parse_client_request(stream);
                                },
                                Err(e) => {
                                    eprintln!("Connection failed in MJ listen_for_requests: {}", e);
                                }
                            }
                        }
                    },
                    Err(e) => {
                        eprintln!("Address parse error in MJ listen_for_requests: {}", e);
                    }
                }
            }, 
            Err(e) => {
                eprintln!("Get ip address error in MJ listen_for_requests: {}", e);
            }
        };

        Ok(())
    }

    fn parse_client_request(&mut self, mut stream: TcpStream) -> Result<()> {
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

    /**
     * Handle requests and acks
     */
    fn execute_request(&mut self, data: Vec<u8>) -> Result<()> {
        match bincode::deserialize::<MessageType>(&data) {
            Ok(MessageType::MapleRequestType(maple_request)) => {
                println!("LEADER GOT MAPLE REQUEST");
                self.handle_maple_request(maple_request);
                Ok(())
            }, 
            Ok(MessageType::JuiceRequestType(juice_request)) => {
                println!("LEADER GOT JUICE REQUEST");
                self.handle_juice_request(juice_request);
                Ok(())
            },
            Ok(MessageType::MapleCompleteAckType(maple_complete_ack)) => {
                println!("LEADER GOT MAPLE COMPLETE ACK");
                self.handle_maple_complete_ack(maple_complete_ack);
                Ok(())
            },
            Ok(MessageType::JuiceCompleteAckType(juice_complete_ack)) => {
                println!("LEADER GOT JUICE COMPLETE ACK");
                self.handle_juice_complete_ack(juice_complete_ack);
                Ok(())
            },
            Ok(MessageType::MapleHandlerRequestType(_maple_handler_request)) => {
                Ok(())
            },
            Ok(MessageType::JuiceHandlerRequestType(_juice_handler_request)) => {
                Ok(())
            },
            Ok(MessageType::FullTaskCompleteAckType(_full_task_complete_ack)) => {
                Ok(())
            }
            Err(e) => {
                println!("ERR: {:?}", e);
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Could not deserialize to any client request or completion ack struct",
                ))
            }
        }
    }

    /**
     * On failure, send replication requests for tasks
     */
    pub fn send_task_replication_requests(&mut self, failed_machine: Arc<String>) {
        let messages = 
            if let Ok(map) = self.machine_to_task_map.lock() {
                if let Some(val) = map.get(&(*failed_machine).clone()) {
                    val.clone()
                } else {
                    eprintln!("MAP DOESN'T CONTAIN THE FAILED MACHINE");
                    Vec::new()
                }
            } else {
                eprintln!("COULDN'T LOCK MAP");
                Vec::new()
            };


        for message in messages.iter() {
            let failed_machine_clone = Arc::clone(&failed_machine);
            match (*message).clone() {
                MessageType::MapleHandlerRequestType(_maple_handler_request) => {
                    self.rereplicate_handler_request((*message).clone(), failed_machine_clone);
                },
                MessageType::JuiceHandlerRequestType(_juice_handler_request) => {
                    self.rereplicate_handler_request((*message).clone(), failed_machine_clone);
                },
                MessageType::MapleRequestType(_maple_request) => {
                    eprintln!("THIS HASHMAP SHOULD NOT CONTAIN THIS TYPE MAPLE_REQUEST");
                },
                MessageType::MapleCompleteAckType(_maple_complete_ack) => {
                    eprintln!("THIS HASHMAP SHOULD NOT CONTAIN THIS TYPE MAPLE_COMPLETE_ACK");
                },
                MessageType::JuiceRequestType(_juice_request) => {
                    eprintln!("THIS HASHMAP SHOULD NOT CONTAIN THIS TYPE JUICE_REQUEST");
                },
                MessageType::JuiceCompleteAckType(_juice_complete_ack) => {
                    eprintln!("THIS HASHMAP SHOULD NOT CONTAIN THIS TYPE JUICE_COMPLETE_ACK");
                },
                MessageType::FullTaskCompleteAckType(_full_task_complete_ack) => {
                    eprintln!("THIS HASHMAP SHOULD NOT CONTAIN THIS TYPE FULL TASK COMPLETE ACK");
                }
            }
        }

        if let Ok(mut map) = self.machine_to_task_map.lock() {
            map.remove(&(*failed_machine));
        }

        if let Ok(mj_task) = self.current_maplejuice_task.lock() {
            if let Some(mut task) = mj_task.clone() {
                task.servers_left_to_complete_task.retain(|x| x != &*failed_machine);
            }
        }
    }

    /**
     * Replicate the handler request that was initially sent
     */
    fn rereplicate_handler_request(&self, handler_request: MessageType, failed_machine: Arc<String>) {
        let leader_ip_address = get_ip_addr(&self.machine_domain_name);

        let mut active_servers: Vec<String> = 
            match self.membership_list.lock() {
                Ok(mem_list) => {
                    match leader_ip_address {
                        Ok(leader_ip) => {
                            mem_list.keys()
                                .filter(|(ip_address_of_machine, _, _)| 
                                    ip_address_of_machine != &*leader_ip &&
                                    ip_address_of_machine != &*failed_machine)
                                .map(|(ip_address_of_machine, _, _)| ip_address_of_machine.clone()).collect()
                        },
                        Err(_) => {
                            eprintln!("COULDN'T MATCH LEADER IP ADDRESS");
                            Vec::new()
                        }
                    }
                },
                Err(_) => {
                    eprintln!("Issue with locking the prefix to keys map!");
                    Vec::new()
                }
            };

        if let Ok(mut map) = self.machine_to_task_map.lock() {
            let set: HashSet<String> = map.keys().cloned().collect();
    
            let set_difference: Vec<String> = active_servers.clone()
                .into_iter()
                .filter(|item| !set.contains(item))
                .collect();

            if set_difference.len() > 0 {
                let ip_to_send_replication = Arc::new(set_difference[0].clone());

                let address_str = format!("{}:{}", ip_to_send_replication.to_string(), MP4_PORT_CLIENT);
                println!("LEADER REPLICATE REQUEST SEND TO ADDRESS IF BLOCK: {}", address_str);

                send_serialized_message(Arc::new(address_str), handler_request.clone());

                if let Ok(mj_task) = self.current_maplejuice_task.lock() {
                    if let Some(mut task) = mj_task.clone() {
                        task.servers_left_to_complete_task.push((*ip_to_send_replication).clone());
                    }
                }

                map.entry((*ip_to_send_replication).clone())
                    .or_insert_with(Vec::new)
                    .push(handler_request.clone());
            } else {
                active_servers.shuffle(&mut thread_rng());
                let ip_to_send_replication = Arc::new(active_servers[0].clone());

                let address_str = format!("{}:{}", ip_to_send_replication.to_string(), MP4_PORT_CLIENT);
                println!("LEADER REREPLICATE REQUEST SEND TO ADDRESS ELSE BLOCK: {}", address_str);

                send_serialized_message(Arc::new(address_str), handler_request);
            }
        }
    }
}