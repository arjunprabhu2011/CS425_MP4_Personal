use crate::leader_mj::LeaderMapleJuice;
use crate::utils_messages_types::{JuiceRequest, JuiceHandlerRequest, JuiceCompleteAck, MJTask, TaskType, FullTaskCompleteAck, MessageType};
use crate::utils_funcs::{get_ip_addr, send_serialized_message};
use crate::utils_consts::MP4_PORT;
use rand::thread_rng;
use rand::seq::SliceRandom;
use std::cmp::min;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::fs::{OpenOptions, self};
use std::io::{Write, Result};
use std::sync::Arc;
use mp3_code::{handle_put, wait_for_put_ack};

impl LeaderMapleJuice {
    pub fn handle_juice_request(&mut self, juice_request: JuiceRequest) {
        if let Ok(mut map) = self.machine_to_task_map.lock() {
            map.clear();
        }

        let prefix = juice_request.sdfs_intermediate_filename_prefix.clone();
        let keys: Vec<String> = 
            match self.sdfs_prefix_to_keys.lock() {
                Ok(prefix_to_keys_map) => {
                    match prefix_to_keys_map.get(&prefix) {
                        Some(key_vec) => {
                            (*key_vec).clone()
                        },
                        None => {
                            eprintln!("Could not find prefix in hashmap with keys!");
                            Vec::new()
                        }
                    }
                },
                Err(_) => {
                    eprintln!("Issue with locking the prefix to keys map!");
                    Vec::new()
                }
            };

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
                    eprintln!("Issue with locking the membership list");
                    Vec::new()
                }
            };

        let num_juices = min(juice_request.num_juices, active_servers.len() as u16);
        active_servers.shuffle(&mut thread_rng());
        let slice_of_active_servers = active_servers[..num_juices as usize].to_vec();
        let mut server_key_map: HashMap<String, Vec<String>> = HashMap::new();
        
        if juice_request.hash_or_range == 0 {
            let self_clone = self.clone();
            self_clone.hash_partition(&keys, num_juices, &slice_of_active_servers, &mut server_key_map);
        } else if juice_request.hash_or_range == 1 {
            let self_clone = self.clone();
            self_clone.range_partition(&keys, num_juices, &slice_of_active_servers, &mut server_key_map)
        }

        let mj_task = MJTask {
            original_client_domain_name: juice_request.original_client_domain_name,
            task_type: TaskType::Juice,
            servers_left_to_complete_task: slice_of_active_servers,
            exe_file: juice_request.juice_exe.clone()
        };

        self.current_maplejuice_task = Some(mj_task);

        for (k, v) in server_key_map.iter() {
            let juice_handler_request = MessageType::JuiceHandlerRequestType(JuiceHandlerRequest {
                juice_exe: juice_request.juice_exe.clone(),
                sdfs_intermediate_filename_prefix: juice_request.sdfs_intermediate_filename_prefix.clone(),
                sdfs_dest_filename: juice_request.sdfs_dest_filename.clone(),
                delete_input: juice_request.delete_input,
                vec_of_keys: (*v).clone()
            });

            match self.machine_to_task_map.lock() {
                Ok(mut map) => {
                    map.entry((*k).clone())
                        .or_insert_with(Vec::new)
                        .push(juice_handler_request.clone());
                },
                Err(_) => {
                    eprintln!("ISSUE LOCKING MACHINE TO TASK MAP IN JUICE HANDLE REQUEST");
                }
            }

            let ip_destination = Arc::new((*k).clone());
            
            let address_str = format!("{}:{}", ip_destination.to_string(), MP4_PORT);

            send_serialized_message(Arc::new(address_str), juice_handler_request.clone());
        }
    }

    fn hash_partition(self, maple_juice_keys_vec: &Vec<String>, num_juice_servers: u16, servers_to_choose_from: &Vec<String>, server_to_key_map: &mut HashMap<String, Vec<String>>) {
        for maple_juice_key in maple_juice_keys_vec.iter() {
            let mut hasher = DefaultHasher::new();
            maple_juice_key.hash(&mut hasher);
            let hashed_value = hasher.finish();

            let index_to_choose = hashed_value % (num_juice_servers as u64); 
            let server_to_choose = servers_to_choose_from[index_to_choose as usize].clone();

            if server_to_key_map.contains_key(&server_to_choose) {
                match server_to_key_map.get_mut(&server_to_choose) {
                    Some(value) => {
                        value.push((*maple_juice_key).clone());
                    },
                    None => {
                        eprintln!("COULDN'T FIND MUTABLE KEY IN MAP OF SERVERS TO KEYS IN HASH PARTITION");
                    }
                }
            } else {
                server_to_key_map.insert(server_to_choose, vec![(*maple_juice_key).clone()]);
            }
        }
    }

    fn range_partition(self, maple_juice_keys_vec: &Vec<String>, num_juice_servers: u16, servers_to_choose_from: &Vec<String>, server_to_key_map: &mut HashMap<String, Vec<String>>) {
        let num_keys = maple_juice_keys_vec.len();
        let partition_size = num_keys / num_juice_servers as usize;
        let mut remaining_keys = num_keys % num_juice_servers as usize;

        let mut start_index = 0;
        let mut end_index = 0;

        for server in servers_to_choose_from.iter() {
            if remaining_keys > 0 {
                end_index = start_index + partition_size + 1;
            } else {
                end_index = start_index + partition_size;
            }
            
            end_index = min(end_index, num_keys);

            let partition = &maple_juice_keys_vec[start_index..end_index];
            server_to_key_map.insert(server.clone(), partition.to_vec());

            start_index = end_index;
            if remaining_keys > 0 { 
                remaining_keys -= 1; 
            }
        }
    }

    pub fn handle_juice_complete_ack(&mut self, juice_ack: JuiceCompleteAck) -> Result<()> {
        let mut output_file = OpenOptions::new()
            .write(true)
            .append(true) // Set to true to append data to the end of the file
            .open("complete_output.txt")?;

        output_file.write_all(&juice_ack.contents_to_output);
        drop(output_file);

        match self.current_maplejuice_task.clone() {
            Some(mut task) => {
                if let Some(index) = task.servers_left_to_complete_task.iter().position(|x| x == &juice_ack.domain_name) {
                    task.servers_left_to_complete_task.remove(index);

                    if task.servers_left_to_complete_task.is_empty() {
                        handle_put(juice_ack.sdfs_dest_filename, "complete_output.txt".to_string(), (*self.machine_domain_name).clone(), self.sdfs_leader_domain_name.clone());
                        
                        wait_for_put_ack(&self.machine_domain_name);

                        fs::remove_file("complete_output.txt");
                        if let Ok(ip) = get_ip_addr(&task.original_client_domain_name) {
                            let full_task_complete_ack = MessageType::FullTaskCompleteAckType(FullTaskCompleteAck {
                                task_type: TaskType::Juice,
                                exe_file: juice_ack.exe_file
                            });

                            let address_str = format!("{}:{}", ip.to_string(), MP4_PORT);

                            send_serialized_message(Arc::new(address_str), full_task_complete_ack);

                            if juice_ack.delete_input == 1 {
                                if let Ok(mut map) = self.sdfs_prefix_to_keys.lock() {
                                    map.remove(&juice_ack.sdfs_intermediate_prefix);
                                }
                            }
                        }
                    }
                }

                Ok(())
            },
            None => {
                eprintln!("THE CURRENT TASK IS NOT INITIALIZED UPON RECEIPT OF A JUICE ACK");
                Ok(())
            }
        }
    }
}