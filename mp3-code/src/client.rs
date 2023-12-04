use std::fs::File;
use std::io::{Write, self, Read};
use std::thread;
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::io::Result;
use std::sync::{Arc, Mutex};
use std::fs::OpenOptions;
use std::time::{Instant, Duration};
use bincode;
use std::mem::drop;
use crate::utils::*;

#[derive(Clone)]
pub struct Client {
    pub leader_domain_name: Arc<Mutex<String>>,
    request_to_shards_processed_hashmap: Arc<Mutex<HashMap<String, Vec<i32>>>>,
    filename_to_shards_responses_hashmap: Arc<Mutex<HashMap<String, Vec<Vec<u8>>>>>,
    pub machine_domain_name: Arc<String>,
    leader_connection_join_handle: Arc<Mutex<Option<thread::JoinHandle<()>>>>, //This handle used for when we want to join the threads
    replica_connection_join_handle: Arc<Mutex<Option<thread::JoinHandle<()>>>>, //This handle used for when we want to join the threads
    leader_ls_join_handle: Arc<Mutex<Option<thread::JoinHandle<()>>>>
}

/**
 * Main structure for the client
 */
impl Client {
    pub fn new(machine_name: Arc<String>, leader_name: Arc<Mutex<String>>) -> Self {
        Client { 
            leader_domain_name: leader_name,
            request_to_shards_processed_hashmap: Arc::new(Mutex::new(HashMap::new())),
            filename_to_shards_responses_hashmap: Arc::new(Mutex::new(HashMap::new())),
            machine_domain_name: machine_name,
            leader_connection_join_handle: Arc::new(Mutex::new(None)),
            replica_connection_join_handle: Arc::new(Mutex::new(None)),
            leader_ls_join_handle: Arc::new(Mutex::new(None))
        } 
    }

    fn update_leader_domain_name(&mut self, new_name: &str) {
        if let Ok(mut leader_name) = self.leader_domain_name.lock() {
            *leader_name = new_name.to_string();
        }
    }

    pub fn iterate_over_shard_map_get(&self, res: Arc<ReplicaListResponse>) {
        for (shard, replica_list) in res.file_shards.iter() {
            // Clone the data itself, not the reference
            let replica_list_clone = Arc::new(replica_list.clone());
            let shard_clone = *shard;  // Assuming that shard is of a type that implements Copy
            let res_clone = Arc::clone(&res);
            let self_clone = self.clone();
    
            thread::spawn(move || {
                // Pass the cloned data to the new thread
                self_clone.send_get_requests_to_replicas(replica_list_clone, res_clone, shard_clone);
            });
        }
    }
    
    pub fn send_get_requests_to_replicas(&self, replica_lst: Arc<Vec<String>>, res: Arc<ReplicaListResponse>, shard_num: u16) {
        let read_req = Arc::new(ReadRequest {
            sdfs_filename: res.sdfs_filename.clone(),
            unix_filename: res.unix_filename.clone(),
            requesting_machine_domain_name: self.machine_domain_name.as_ref().clone(),
            shard_number: shard_num,
            request_type: res.request_type_to_send.clone() 
        });

        let read_req_clone = Arc::clone(&read_req);

        if let Ok(mut map) = self.request_to_shards_processed_hashmap.lock() {
            let zeros: Vec<i32> = vec![0; NUM_SHARDS as usize];
            map.insert(read_req_clone.sdfs_filename.clone(), zeros);
        }

        println!("ARJUNNNNNNNN");

        if let Ok(mut map) = self.filename_to_shards_responses_hashmap.lock() {
            println!("ENTERRED 7777777");
            let mut num_shard_vecs: Vec<Vec<u8>> = Vec::with_capacity(NUM_SHARDS as usize);

            for _ in 0..NUM_SHARDS {
                num_shard_vecs.push(Vec::new());
            }

            map.insert(read_req_clone.unix_filename.clone(), num_shard_vecs);
            println!("INSERT INTO CLIENT SHARDS MAP: {:?}", map);
        }

        for rep in replica_lst.iter() {
            let read_req_clone = Arc::clone(&read_req);
            let self_clone = self.clone();

            let rep_clone = rep.clone();

           thread::spawn(move || {
                self_clone.send_single_get_request_to_replica(read_req_clone, rep_clone);
            });
        }
    }

    pub fn send_single_get_request_to_replica(&self, req: Arc<ReadRequest>, replica: String) -> Result<()> {
        let ip_addr = get_ip_addr(&replica);
    
        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:8011", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_address) => {
                        if let Ok(mut stream) = TcpStream::connect(socket_address) {
                            // Dereference the Arc and clone the inner object to get a value to pass
                            let request_value = (*req).clone();
                            let client_request = ClientRequest::Read(request_value);
    
                            // Now serialize the client_request instead of the raw ReadRequest
                            let serialized_request = bincode::serialize(&client_request);
    
                            match serialized_request {
                                Ok(ser_req) => {
                                    println!("SENT GET REQUEST TO A REPLICA");
                                    stream.write_all(&ser_req)?;
                                },
                                Err(_) => {
                                    eprintln!("Error in serializing the read request");
                                }
                            }
                        } else {
                            println!("Failed to TCP send message SEND SINGLE GET REQUEST TO REPLICA");
                        }
                    },
                    Err(_) => {
                        eprintln!("Issue with parsing address");
                    }
                }
            },
            Err(_) => {
                eprintln!("Issue with getting ip address");
            }
        }
    
        Ok(())
    }
    

    pub fn iterate_over_shard_map_delete(&self, res: Arc<ReplicaListResponse>) {
        for (shard, replica_list) in &res.file_shards {
            let replica_list_clone = replica_list.clone();
            let shard_clone = *shard; 
            let res_clone = Arc::clone(&res);
            let self_clone = self.clone();
    
            thread::spawn(move || {
                self_clone.send_delete_requests_to_replicas(replica_list_clone, res_clone, shard_clone);
            });
        }
    }
    

    pub fn send_delete_requests_to_replicas(&self, replica_lst: Vec<String>, res: Arc<ReplicaListResponse>, shard_num: u16) {
        let delete_req = Arc::new(DeleteRequest {
            filename: res.sdfs_filename.clone(),
            requesting_machine_domain_name: self.machine_domain_name.as_ref().clone(),
            shard_number: shard_num,
            request_type: res.request_type_to_send.clone()
        });

        let delete_req_clone = Arc::clone(&delete_req);

        if let Ok(mut map) = self.request_to_shards_processed_hashmap.lock() {
            let zeros: Vec<i32> = vec![0; NUM_SHARDS as usize];
            map.insert(delete_req_clone.filename.clone(), zeros);
        }

        for rep in replica_lst.iter() {
            let delete_req_clone = Arc::clone(&delete_req);
            let self_clone = self.clone();
            let rep_clone = rep.clone();

           thread::spawn(move || {
                self_clone.send_single_delete_request_to_replica(delete_req_clone, rep_clone);
            });
        }
    }

    pub fn send_single_delete_request_to_replica(&self, req: Arc<DeleteRequest>, replica: String) -> Result<()> {
        let ip_addr = get_ip_addr(&replica);
    
        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:8011", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_address) => {
                        if let Ok(mut stream) = TcpStream::connect(socket_address) {
                            // Dereference the Arc and clone the inner object to get a value to pass
                            let request_value = (*req).clone();
                            let client_request = ClientRequest::Delete(request_value);
    
                            // Now serialize the client_request instead of the raw DeleteRequest
                            let serialized_request = bincode::serialize(&client_request);
    
                            match serialized_request {
                                Ok(ser_req) => {
                                    println!("SEND DELETE REQUEST TO REPLICA");
                                    stream.write_all(&ser_req)?;
                                },
                                Err(_) => {
                                    eprintln!("Error in serializing the delete request");
                                }
                            }
                        } else {
                            println!("Failed to TCP send message SEND SINGLE DELETE REQUEST TO REPLICA");
                        }
                    },
                    Err(_) => {
                        eprintln!("Issue with parsing address");
                    }
                }
            },
            Err(_) => {
                eprintln!("Issue with getting ip address");
            }
        }
    
        Ok(())
    }
    

    pub fn iterate_over_shard_map_put(&self, res: Arc<ReplicaListResponse>) -> Result<()> {
        match File::open(res.unix_filename.clone()) {
            Ok(_) => {
                println!("FILE OPEN WORKED");
            },
            Err(e) => {
                println!("FILE OPEN ERROR: {:?}", e)
            }
        }

        let mut file = File::open(res.unix_filename.clone())?;
        println!("ITERATE ZERO");
        let mut file_data = Vec::new();
        file.read_to_end(&mut file_data)?;
    
        let shard_size = file_data.len() / (NUM_SHARDS as usize);
        let mut start_index = 0;
        let mut end_index = shard_size;
        println!("ITERATE FIRST");
    
        let mut shard_to_data_map: HashMap<u16, Vec<u8>> = HashMap::new();
    
        for i in 0..NUM_SHARDS {
            if i == NUM_SHARDS - 1 {
                println!("ITERATE SECOND");
                end_index = file_data.len();
            }
    
            shard_to_data_map.insert(i as u16, file_data[start_index..end_index].to_vec());
            start_index = end_index;
            end_index += shard_size;
        }
    
        for (shard, replica_list) in &res.file_shards {
            let replica_list_clone = replica_list.clone();
            let res_clone = Arc::clone(&res);
            let shard_clone = *shard;
            let self_clone = self.clone();
    
            // Clone the data for this shard to move into the thread.
            let data_to_send = shard_to_data_map.get(&(shard - 1)).cloned().unwrap_or_default();
            
            println!("ITERATE THIRD");
            thread::spawn(move || {
                self_clone.send_put_requests_to_replicas(replica_list_clone, res_clone, shard_clone, data_to_send);
            });
        }
    
        Ok(())
    }
    

    pub fn send_put_requests_to_replicas(&self, replica_lst: Vec<String>, res: Arc<ReplicaListResponse>, shard_num: u16, shard_data: Vec<u8>) {
        println!("SEND PUT REQUESTS FIRST");
        let write_req = Arc::new(WriteRequest {
            sdfs_filename: res.sdfs_filename.clone(),
            unix_filename: res.unix_filename.clone(),
            data: shard_data,
            requesting_machine_domain_name: self.machine_domain_name.as_ref().clone(),
            shard_number: shard_num,
            request_type: res.request_type_to_send.clone()
        });

        println!("SEND PUT REQUESTS SECOND");

        let write_req_clone = Arc::clone(&write_req);

        if let Ok(mut map) = self.request_to_shards_processed_hashmap.lock() {
            let zeros: Vec<i32> = vec![0; NUM_SHARDS as usize];
            map.insert(write_req_clone.sdfs_filename.clone(), zeros);
        }

        println!("SEND PUT REQUESTS THIRD");

        for rep in replica_lst.iter() {
            let write_req_clone = Arc::clone(&write_req);
            let self_clone = self.clone();
            let rep_clone = rep.clone();

            println!("SEND PUT REQUESTS FOURTH");

            thread::spawn(move || {
                self_clone.send_single_put_request_to_replica(write_req_clone, rep_clone);
            });
        }
    }

    pub fn send_single_put_request_to_replica(&self, req: Arc<WriteRequest>, replica: String) -> Result<()> {
        let ip_addr = get_ip_addr(&replica);

        println!("SEND SINGLE PUT FIRST");
    
        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:8011", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_address) => {
                        if let Ok(mut stream) = TcpStream::connect(socket_address) {
                            // Dereference the Arc and clone the inner object to get a value to pass
                            let request_value = (*req).clone();
                            let client_request = ClientRequest::Write(request_value);
                            println!("SEND SINGLE PUT SECOND");
    
                            // Now serialize the client_request instead of the raw WriteRequest
                            let serialized_request = bincode::serialize(&client_request);
    
                            match serialized_request {
                                Ok(ser_req) => {
                                    println!("SEND PUT REQUEST TO REPLICA");
                                    stream.write_all(&ser_req)?;
                                },
                                Err(_) => {
                                    eprintln!("Error in serializing the put request");
                                }
                            }
                        } else {
                            println!("Failed to TCP send message SEND SINGLE PUT REQUEST TO REPLICA");
                        }
                    },
                    Err(_) => {
                        eprintln!("Issue with parsing address");
                    }
                }
            },
            Err(_) => {
                eprintln!("Issue with getting ip address");
            }
        }
    
        Ok(())
    }
    

    pub fn parse_replica_list_response(&self, res: Arc<ReplicaListResponse>) -> Result<()> {
        match &res.request_type_to_send {
            RequestType::Read => {
                self.iterate_over_shard_map_get(res);
                return Ok(());
            },
            RequestType::Write => {
                println!("ENTERRED PARSE REPLICA BRANCH");
                println!("REPLISTRESP: {:?}", res);
                self.iterate_over_shard_map_put(res);
                return Ok(());
            },
            RequestType::Delete => {
                self.iterate_over_shard_map_delete(res);
                return Ok(());
            },
            x => {
                println!("PARSE REPLICA ERR: {:?}", x);
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Could not deserialize to any of client possible responses",
                )); 
            }
        }
    }

    pub fn handle_leader_message_with_replica_list(&self, mut stream: TcpStream) -> Result<()> {
        let mut data = Vec::new();
        let mut buffer = [0; 4096];

        loop {
            let bytes_read = stream.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }

            data.extend_from_slice(&buffer[..bytes_read]);
        }
    
        if let Ok(value) = bincode::deserialize::<ReplicaListResponse>(&data) {
            let value_arc = Arc::new(value);
            self.parse_replica_list_response(value_arc);
            return Ok(());
        } else {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Could not deserialize to Node Failure Request",
            ));
        }
    }

    pub fn listen_to_leader_messages(&self) -> Result<()> {
        let ip_addr = get_ip_addr(&self.machine_domain_name);

        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:8018", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_addr) => {
                        let listener = TcpListener::bind(socket_addr)?;

                        for stream in listener.incoming() {
                            match stream {
                                Ok(stream) => {
                                    println!("RECEIVED REPLICA LIST RESPONSE FROM LEADER");
                                    self.handle_leader_message_with_replica_list(stream);
                                },
                                Err(e) => {
                                    eprintln!("Connection failed in listen_to_client_requests: {}", e);
                                }
                            }
                        }
                    },
                    Err(e) => {
                        eprintln!("Address parse error in listen_to_client_requests: {}", e);
                    }
                }
            }, 
            Err(e) => {
                eprintln!("Get ip address error in listen_to_client_requests: {}", e);
            }
        };

        Ok(())
    }

    pub fn listen_to_replica_messages(&self) -> Result<()> {
        let ip_addr = get_ip_addr(&self.machine_domain_name);

        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:8012", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_addr) => {
                        let listener = TcpListener::bind(socket_addr)?;

                        for stream in listener.incoming() {
                            match stream {
                                Ok(stream) => {
                                    println!("RECEIVED REPLICA RESPONSE");
                                    self.handle_replica_responses(stream);
                                },
                                Err(e) => {
                                    eprintln!("Connection failed in listen_to_client_requests: {}", e);
                                }
                            }
                        }
                    },
                    Err(e) => {
                        eprintln!("Address parse error in listen_to_client_requests: {}", e);
                    }
                }
            }, 
            Err(e) => {
                eprintln!("Get ip address error in listen_to_client_requests: {}", e);
            }
        };

        Ok(())
    }

    fn read_response_helper(&self, res: ReadResponse) {
        println!("INSIDE READ RESP HELPER");
        println!("READ RESP: {:?}", res);
        if let Ok(mut locked_map) = self.filename_to_shards_responses_hashmap.lock() {
            println!("ENTERRS LOCKED READ RESP");
            println!("LOCKED APPPPPPP: {:?}", locked_map);
            // Borrow `res.filename` instead of moving it
            let k = &res.unix_filename;
    
            if !locked_map.contains_key(k) {
                println!("NO KEYYY: {:?}", k);
                drop(locked_map);
                return;
            }
    
            let mut changed = false;
    
            if let Some(shard_vector) = locked_map.get_mut(k) {
                println!("SHARD VEC!!!: {:?}", shard_vector);
                if shard_vector[res.shard_number as usize - 1].is_empty() {
                    shard_vector[res.shard_number as usize - 1] = res.data;
                    changed = true;
                }
            }
    
            if changed {
                if let Some(shard_vector) = locked_map.get(k) {
                    if shard_vector.iter().all(|vec| !vec.is_empty()) {
                        // Pass a reference to `shard_vector` and `k`
                        println!("BEFORE WRITE DATA");
                        self.write_data_to_file(shard_vector, k);

                        println!("Get Request Completed!");
    
                        // Clone `k` here to get an owned `String`
                        let req_ack = RequestAck {
                            ack_type: AckType::Read,
                            filename: k.clone(),
                            machine_domain_name: self.machine_domain_name.as_ref().clone(),
                            new_machine_containing_shard: "".to_string(),
                            shard_num: 6
                        };
    
                        self.send_ack_to_leader(req_ack);
    
                        if locked_map.remove(k).is_some() {
                            println!("Removed key {}", k);
                        } else {
                            println!("Key {} did not exist.", k);
                        }
                    }
                }
            }
        }
    }
    

    fn write_data_to_file(&self, shards: &[Vec<u8>], filename: &str) -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(filename)?;
    
        for vec in shards {
            file.write_all(vec)?;
        }
    
        Ok(())
    }
    

    fn send_ack_to_leader(&self, ack: RequestAck) -> Result<()> {
        if let Ok(leader_name) = self.leader_domain_name.lock() {
            let ip_addr = get_ip_addr(&leader_name);

            match ip_addr {
                Ok(ip) => {
                    let address_str = format!("{}:8019", ip.to_string());
                    match address_str.parse::<SocketAddr>() {
                        Ok(socket_address) => {
                            if let Ok(mut stream) = TcpStream::connect(socket_address) {
                                let serialized_request = bincode::serialize(&ack);

                                match serialized_request {
                                    Ok(ser_req) => {
                                        println!("SENT ACK BACK TO LEADER");
                                        stream.write(&ser_req);
                                    },
                                    Err(_) => {
                                        eprintln!("Error in sending serialized response in send_shard_to_other_replica");
                                    }
                                }
                            } else {
                                println!("Failed to TCP send message SEND ACK TO LEADER SDFS");
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

    fn write_response_helper(&self, write_res: WriteResponse) {
        if let Ok(mut locked_map) = self.request_to_shards_processed_hashmap.lock() {
            let k = &write_res.filename;

            if !locked_map.contains_key(k) {
                println!("NO LONGER HAS KEY");
                drop(locked_map);
                return;
            }

            let mut changed = false;

            if let Some(responses_vec) = locked_map.get_mut(k) {
                if responses_vec[write_res.shard_number as usize - 1] == 0 {
                    responses_vec[write_res.shard_number as usize - 1] = 1;
                    changed = true;
                }
            } 

            if changed == true {
                if let Some(responses_vec) = locked_map.get(k) {
                    if responses_vec.iter().all(|&response_status| response_status == 1) {
                        let req_ack = RequestAck {
                            ack_type: AckType::Write,
                            filename: k.clone(),
                            machine_domain_name: self.machine_domain_name.as_ref().clone(),
                            new_machine_containing_shard: "".to_string(),
                            shard_num: 6
                        };

                        println!("PUT REQUEST SUCCEEDED");
        
                        self.send_ack_to_leader(req_ack);

                        if let Some(_) = locked_map.remove(k) {
                            println!("Removed key {}", k);
                        } else {
                            println!("Key {} did not exist.", k);
                        }
                    }
                } 
            }
        }
    }

    fn delete_response_helper(&self, delete_res: DeleteResponse) {
        if let Ok(mut locked_map) = self.request_to_shards_processed_hashmap.lock() {
            let k = &delete_res.filename;

            if !locked_map.contains_key(k) {
                println!("NO LONGER HAS KEY");
                drop(locked_map);
                return;
            }

            let mut changed = false;

            if let Some(responses_vec) = locked_map.get_mut(k) {
                if responses_vec[delete_res.shard_number as usize - 1] == 0 {
                    responses_vec[delete_res.shard_number as usize - 1] = 1;
                    changed = true;
                }
            } 

            if changed == true {
                if let Some(responses_vec) = locked_map.get(k) {
                    if responses_vec.iter().all(|&response_status| response_status == 1) {
                        let req_ack = RequestAck {
                            ack_type: AckType::Write,
                            filename: k.clone(),
                            machine_domain_name: self.machine_domain_name.as_ref().clone(),
                            new_machine_containing_shard: "".to_string(), 
                            shard_num: 6
                        };

                        println!("DELETE REQUEST SUCCEEDED!");
        
                        self.send_ack_to_leader(req_ack);

                        if let Some(_) = locked_map.remove(k) {
                            println!("Removed key {}", k);
                        } else {
                            println!("Key {} did not exist.", k);
                        }
                    }
                } 
            }
        }
    }

    fn handle_replica_responses(&self, mut stream: TcpStream) -> Result<()> {
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
            Ok(ResponseWrapper::Read(value)) => {
                println!("REPLICA READDDDDDDD");
                self.read_response_helper(value);
                Ok(())
            },
            Ok(ResponseWrapper::Write(value)) => {
                println!("REPLICA WRITTTTEEEEE");
                self.write_response_helper(value);
                Ok(())
            },
            Ok(ResponseWrapper::Delete(value)) => {
                self.delete_response_helper(value);
                Ok(())
            },
            Ok(_) => {
                println!("Wrong response type at this port");
                Ok(())
            },
            Err(_) => {
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Could not deserialize to any response struct",
                ))
            },
        }
        
    }

    fn listen_to_ls_responses(&self) -> Result<()> {
        let ip_addr = get_ip_addr(&self.machine_domain_name);

        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:8021", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_addr) => {
                        let listener = TcpListener::bind(socket_addr)?;

                        for stream in listener.incoming() {
                            match stream {
                                Ok(stream) => {
                                    println!("GOT BACK LS RESPONSE FROM LEADER");
                                    self.print_ls_to_terminal(stream);
                                },
                                Err(e) => {
                                    eprintln!("Connection failed in listen_to_client_requests: {}", e);
                                }
                            }
                        }
                    },
                    Err(e) => {
                        eprintln!("Address parse error in listen_to_client_requests: {}", e);
                    }
                }
            }, 
            Err(e) => {
                eprintln!("Get ip address error in listen_to_client_requests: {}", e);
            }
        };

        Ok(())
    }

    pub fn print_ls_to_terminal(&self, mut stream: TcpStream) -> Result<()> {
        let mut data = Vec::new();
        let mut buffer = [0; 4096];

        loop {
            let bytes_read = stream.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }

            data.extend_from_slice(&buffer[..bytes_read]);
        }
    
        if let Ok(value) = bincode::deserialize::<LsResponse>(&data) {
            for s in value.file_shards {
                println!("Shard {} for file {}: Stored at {:?}", s.0, value.filename, s.1);
            }
            return Ok(());
        } else {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Could not deserialize to LsResponse",
            ));
        }
    }

    pub fn start_listening(&mut self) {
        let self_clone_first = self.clone();
        let self_clone_second = self.clone();
        let self_clone_third = self.clone();

        let leader_thread_handle = thread::spawn(move || {
            self_clone_first.listen_to_leader_messages();
        });

        let replica_thread_handle = thread::spawn(move || {
            self_clone_second.listen_to_replica_messages();
        });

        let ls_thread_handle = thread::spawn(move || {
            self_clone_third.listen_to_ls_responses();
        });

        if let Ok(mut leader_join_handle) = self.leader_connection_join_handle.lock() {
            *leader_join_handle = Some(leader_thread_handle);
        }

        if let Ok(mut replica_join_handle) = self.replica_connection_join_handle.lock() {
            *replica_join_handle = Some(replica_thread_handle);
        }

        if let Ok(mut ls_join_handle) = self.leader_ls_join_handle.lock() {
            *ls_join_handle = Some(ls_thread_handle);
        }
    }

    pub fn close_threads(&mut self) {
        if let Ok(mut handle) = self.leader_connection_join_handle.lock() {
            if let Some(hdl) = handle.take() {
                hdl.join().expect("Client connection thread in Replica panicked!");
            }
        }

        if let Ok(mut handle) = self.replica_connection_join_handle.lock() {
            if let Some(hdl) = handle.take() {
                hdl.join().expect("Leader connection thread in Replica panicked!");
            }
        }

        if let Ok(mut handle) = self.leader_ls_join_handle.lock() {
            if let Some(hdl) = handle.take() {
                hdl.join().expect("Leader connection thread in Replica panicked!");
            }
        }
    }
}