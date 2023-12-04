use std::cmp::min;
use std::fs::{File, OpenOptions, self};
use std::io::{Result, BufReader, BufRead, Write};
use std::sync::Arc;
use std::thread;
use crate::leader_mj::LeaderMapleJuice;
use mp3_code::{handle_get, wait_for_get_ack, handle_put, wait_for_put_ack};
use crate::utils_messages_types::{MapleRequest, MapleHandlerRequest, MapleCompleteAck, MessageType, MJTask, TaskType, FullTaskCompleteAck};
use crate::utils_funcs::{split_file, get_ip_addr, send_serialized_message};
use crate::utils_consts::MP4_PORT_CLIENT;
use rand::thread_rng;
use rand::seq::SliceRandom;

/**
 * Struct representing the maple juice leader's maple actions
 */
impl LeaderMapleJuice {
    /**
     * Handle a maple request from a client
     */
    pub fn handle_maple_request(&mut self, maple_request: MapleRequest) {
        println!("Received Maple Request: {:?}", maple_request);

        if let Ok(mut map) = self.machine_to_task_map.lock() {
            map.clear();
        }

        let source_name = &maple_request.source_file;

        handle_get((*source_name).clone(), "temp_input_data.csv".to_string(), (*self.machine_domain_name).clone(), self.sdfs_leader_domain_name.clone());

        let ret_get_ack = wait_for_get_ack(&self.machine_domain_name);

        if let Ok(true) = ret_get_ack { 
            let leader_ip_address = get_ip_addr(&self.machine_domain_name);

            let mut active_servers: Vec<String> = 
                match self.membership_list.lock() {
                    Ok(mem_list) => {
                        match leader_ip_address {
                            Ok(leader_ip) => {
                                mem_list.keys()
                                    .filter(|(ip_address_of_machine, _, _)| ip_address_of_machine != &*leader_ip)
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

            let num_maples = min(maple_request.num_maples, active_servers.len() as u16);
            active_servers.shuffle(&mut thread_rng());

            let split_file_indices = split_file("temp_input_data.csv", num_maples as u32);

            let mj_task = MJTask {
                original_client_domain_name: maple_request.original_client_domain_name,
                task_type: TaskType::Maple,
                servers_left_to_complete_task: active_servers[..num_maples as usize].to_vec(),
                exe_file: maple_request.maple_exe.clone()
            };

            if let Ok(mut task) = self.current_maplejuice_task.lock() {
               *task = Some(mj_task);
            }

            let mut index_to_pick = 0;

            if let Ok(val) = split_file_indices {
                for index in val.iter() {
                    let start_index = index.0;
                    let end_index = index.1;
                    println!("START_INDEX, END_INDEX: {}, {}", start_index, end_index);

                    let split_of_data = self.read_lines_range_as_bytes("temp_input_data.csv", start_index, end_index);
                    
                    if let Ok(split_data) = split_of_data {
                        let ip_destination = Arc::new(active_servers[index_to_pick].clone());
                        let maple_handler_request = MessageType::MapleHandlerRequestType(MapleHandlerRequest {
                            maple_exe: maple_request.maple_exe.clone(),
                            intermediate_prefix: maple_request.intermediate_prefix.clone(),
                            sql_file: maple_request.sql_file.clone(),
                            input_data: split_data,
                            start_line: start_index
                        });

                        let address_str = format!("{}:{}", ip_destination.to_string(), MP4_PORT_CLIENT);
                        println!("HANDLE MAPLE CLIENT REQUEST ADDRESS (SEND): {}", address_str);

                        send_serialized_message(Arc::new(address_str), maple_handler_request.clone());

                        match self.machine_to_task_map.lock() {
                            Ok(mut map) => {
                                map.entry((*ip_destination).clone())
                                    .or_insert_with(Vec::new)
                                    .push(maple_handler_request.clone());
                            },
                            Err(_) => {
                                eprintln!("ISSUE LOCKING MACHINE TO TASK MAP IN MAPLE HANDLE REQUEST");
                            }
                        }
                    }

                    index_to_pick += 1;
                }
            }

            match self.sdfs_prefix_to_keys.lock() {
                Ok(mut map) => {
                    map.insert(maple_request.intermediate_prefix, Vec::new());
                },
                Err(_) => {
                    eprintln!("ISSUE LOCKING SDFS PREFIX TO KEYS MAP IN MAPLE HANDLE REQUEST");
                }
            }
        }
    }

    /**
     * Read lines from the input data from start_line to end_line
     */
    fn read_lines_range_as_bytes(&self, file_path: &str, start_line: u64, end_line: u64) -> Result<Vec<u8>> {
        let file = File::open(file_path)?;
        let reader = BufReader::new(file);
    
        let mut bytes = Vec::new();
        let mut line_number = 0;
    
        for line in reader.lines() {
            println!("LINEEEEE: {:?}", line);
            line_number += 1;
    
            if line_number >= start_line && line_number <= end_line {
                let line = line?;
                bytes.extend_from_slice(line.as_bytes());
    
                // Add a newline character after each line, except the last one
                if line_number != end_line {
                    bytes.push(b'\n');
                }
            }
    
            if line_number > end_line {
                break;
            }
        }
    
        Ok(bytes)
    }

    /**
     * Handle an ack saying the maple request was successful
     */
    pub fn handle_maple_complete_ack(&mut self, maple_complete_ack: MapleCompleteAck) {
        if let Ok(mut mj_task) = self.current_maplejuice_task.lock() {
            match mj_task.as_mut() {
                Some(mut task) => {
                    println!("SERVERS COMPLETE TASK BEFORE REMOVAL: {:?}", task.servers_left_to_complete_task);
                    println!("MAPLE ACK DOMAIN NAME: {}", maple_complete_ack.domain_name);
                    if let Ok(ip) = get_ip_addr(&maple_complete_ack.domain_name) {
                        if let Some(index) = task.servers_left_to_complete_task.iter().position(|x| x == &*ip) {
                            task.servers_left_to_complete_task.remove(index);
                            println!("SERVERS STILL LEFT TO COMPLETE MAPLE HANDLER REQUEST: {:?}", task.servers_left_to_complete_task);
        
                            if let Ok(mut map) = self.key_to_all_values_map.lock() {
                                for (k, v) in maple_complete_ack.key_to_values_output.iter() {
                                    for val in v.iter() {
                                        map.entry((*k).clone())
                                            .or_insert_with(Vec::new)
                                            .push((*val).clone());
                                    }
                                }
                            }
        
                            if task.servers_left_to_complete_task.is_empty() {
                                if let Ok(map) = self.key_to_all_values_map.lock() {
                                    for (k,v) in map.iter() {
                                        let k_clone = k.clone();
                                        let v_clone = v.clone();
                                        let sdfs_intermediate_prefix_clone = maple_complete_ack.sdfs_intermediate_prefix.clone();
                                        let machine_domain_name_clone = self.machine_domain_name.clone();
                                        let sdfs_leader_domain_name_clone = self.sdfs_leader_domain_name.clone();
        
                                        if let Ok(mut pref_map) = self.sdfs_prefix_to_keys.lock() {
                                            let entry = pref_map.entry(sdfs_intermediate_prefix_clone.clone())
                                                .or_insert_with(Vec::new);
                                            
                                            if !entry.contains(&k_clone) {
                                                entry.push(k_clone.clone());
                                            }
                                        } 
        
                                        let put_request_thread_handle = thread::spawn(move || {
                                            let local_file_name = format!("leader_local_{}_{}.txt", sdfs_intermediate_prefix_clone, k_clone);
                                            let output_file = File::create(local_file_name.clone());
        
                                            {
                                                if let Ok(mut file) = output_file {
                                                    for value in v_clone.iter() {
                                                        file.write_all(value);
                                                    }
                                                }
                                            }
        
                                            let sdfs_file_name_for_key = format!("{}_{}.txt", sdfs_intermediate_prefix_clone, k_clone);
        
                                            handle_put(sdfs_file_name_for_key, local_file_name.clone(), (*machine_domain_name_clone).clone(), sdfs_leader_domain_name_clone);
        
                                            let put_ack_ret = wait_for_put_ack(&*machine_domain_name_clone);
                                        });
                                    }
                                }
        
                                if let Ok(ip) = get_ip_addr(&task.original_client_domain_name) {
                                    let full_task_complete_ack = MessageType::FullTaskCompleteAckType(FullTaskCompleteAck {
                                        task_type: TaskType::Maple,
                                        exe_file: maple_complete_ack.exe_file
                                    });
        
                                    let address_str = format!("{}:{}", ip.to_string(), MP4_PORT_CLIENT);
                                    println!("SEND FULL ACK BACK TO CLIENT ADDRESS: {}", address_str);
        
                                    send_serialized_message(Arc::new(address_str), full_task_complete_ack);
                                }
                            }
                        }
                    }
                },
                None => {
                    eprintln!("THE CURRENT TASK IS NOT INITIALIZED UPON RECEIPT OF A MAPLE ACK");
                }
            }
        }
    }    

}
