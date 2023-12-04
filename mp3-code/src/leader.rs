use std::time::SystemTime;
use std::io::{Write, self, Read};
use std::thread;
use std::collections::{HashMap, HashSet};
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::io::Result;
use std::sync::{Arc, Mutex};
use bincode;
use rand::Rng;

use crate::utils::*;
pub use crate::NodeStatus;

#[derive(Clone)]
pub struct FileMetadata {
    currently_writing: Arc<Mutex<bool>>,
    current_reads: Arc<Mutex<u16>>
}

impl FileMetadata {
    pub fn new() -> Self {
        FileMetadata { 
            currently_writing: Arc::new(Mutex::new(false)),
            current_reads: Arc::new(Mutex::new(0))
        } 
    }
}

#[derive(Clone)]
pub struct LeaderListener {
    hold_requests: Arc<Mutex<Vec<ClientRequest>>>,
    filename_to_metadata_map: Arc<Mutex<HashMap<String, FileMetadata>>>,
    filename_to_locations_map: Arc<Mutex<HashMap<String, HashMap<u16, Vec<String>>>>>, 
    machine_to_file_shards_map: Arc<Mutex<HashMap<String, Vec<(String, u16)>>>>, 
    consecutive_read_requests: Arc<Mutex<u16>>,
    consecutive_write_requests: Arc<Mutex<u16>>,
    client_connection_join_handle: Arc<Mutex<Option<thread::JoinHandle<()>>>>, //This handle used for when we want to join the threads
    client_ack_join_handle: Arc<Mutex<Option<thread::JoinHandle<()>>>>, //This handle used for when we want to join the threads
    client_ls_join_handle: Arc<Mutex<Option<thread::JoinHandle<()>>>>, //This handle used for when we want to join the threads
    replica_ack_join_handle: Arc<Mutex<Option<thread::JoinHandle<()>>>>, //This handle used for when we want to join the threads
    pub machine_domain_name: Arc<String>,
    membership_list: Arc<Mutex<HashMap<(String, usize, SystemTime), (u64, u64, NodeStatus)>>>
}

/**
 * Main structure of the Leader to listen and manage the sdfs
 */
impl LeaderListener {
    pub fn new(machine_name: Arc<String>, mem_list: Arc<Mutex<HashMap<(String, usize, SystemTime), (u64, u64, NodeStatus)>>>) -> Self {
        LeaderListener { 
            hold_requests: Arc::new(Mutex::new(Vec::new())),
            filename_to_metadata_map: Arc::new(Mutex::new(HashMap::new())),
            filename_to_locations_map: Arc::new(Mutex::new(HashMap::new())),
            machine_to_file_shards_map: Arc::new(Mutex::new(HashMap::new())),
            consecutive_read_requests: Arc::new(Mutex::new(0)),
            consecutive_write_requests: Arc::new(Mutex::new(0)),
            client_connection_join_handle: Arc::new(Mutex::new(None)), 
            client_ack_join_handle: Arc::new(Mutex::new(None)), 
            client_ls_join_handle: Arc::new(Mutex::new(None)), 
            replica_ack_join_handle: Arc::new(Mutex::new(None)), 
            machine_domain_name: machine_name,
            membership_list: mem_list
        } 
    }

    pub fn send_all_node_failure_requests(&self, machine_ip_num: Arc<String>) {
        let mut domain_name_of_failed_machine = "".to_string();
        for d in DOMAINS {
            if let Ok(ip) = get_ip_addr(d) {
                if ip == machine_ip_num {
                    domain_name_of_failed_machine = d.to_string();
                    break;  // Assuming domain names are unique, we can break after finding the match.
                }
            }
        }

        println!("FAILLLDD MACHINNNNEEEEEE: {:?}", domain_name_of_failed_machine);
    
        let domain_name_arc = Arc::new(domain_name_of_failed_machine);
    
        if let Ok(map) = self.machine_to_file_shards_map.lock() {
            if let Some(val) = map.get(&*domain_name_arc) {
                println!("SHARDS THAT FAILED MACHINE STORED: {:?}", val);
                // Collect data to be used for spawning threads
                let mut requests_to_send = Vec::new();
    
                for (file, shard) in val.iter().cloned() {
                    if let Ok(mut map2) = self.filename_to_locations_map.lock() {
                        if let Some(shards_map) = map2.get_mut(&file) {
                            if let Some(machines_containing_shard) = shards_map.get_mut(&shard) {
                                println!("MACHINES CONTAINING FILE {:?} : {:?}", file, machines_containing_shard);
                                // Clone the machines_containing_shard to use outside the lock
                                let machines_to_send = machines_containing_shard.clone();
                                let file = file.clone();
                                let shard = shard;
    
                                if let Ok(mem_list) = self.membership_list.lock() {
                                    println!("ENTER LOCKED MEM LIST LEADER 95");
                                    let keys: Vec<(String, usize, SystemTime)> = mem_list.keys().cloned().collect();
                                    let mut rng = rand::thread_rng();
    
                                    let number = loop {
                                        let temp_number = rng.gen_range(1..=keys.len());
    
                                        if !machines_to_send.contains(&keys[temp_number - 1].0) {
                                            if keys[temp_number - 1].0 != *domain_name_arc {
                                                println!("BREAK FINALLLYYYYYY");
                                                break temp_number;
                                            }
                                        }
                                    };

                                    println!("NEW MACHINNNEEEE: {:?}", number);
    
                                    // Prepare the requests to be sent after releasing the lock
                                    for machine in machines_to_send {
                                        if machine == *domain_name_arc {
                                            continue;
                                        }
    
                                        requests_to_send.push((file.clone(), shard, keys[number - 1].0.clone(), machine));
                                    }
                                }
                            }
                        }
                    }
                }

                println!("REQUESTS TO SEND: {:?}", requests_to_send);
    
                // Now that we have all the data, we can spawn threads without holding onto the lock
                for (file, shard, new_machine, target_machine) in requests_to_send {
                    let node_failure_req = NodeFailureRequest {
                        filename: file,
                        shard_number: shard,
                        machine_domain_name_to_send_shard_to: new_machine,
                        request_type: RequestType::NodeFailure,
                    };

                    println!("NODE FAILURE REQUEST: {:?}", node_failure_req);
    
                    let self_clone = self.clone();
    
                    thread::spawn(move || {
                        self_clone.send_node_failure_request(Arc::new(node_failure_req), target_machine);
                    });
                }
            } else {
                println!("Failed machine not stored at leader before failure");
            }
        }
    }
    

    pub fn send_node_failure_request(&self, req: Arc<NodeFailureRequest>, replica_machine_to_start_replication: String) -> Result<()> {
        let ip_addr = get_ip_addr(&replica_machine_to_start_replication);
        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:8013", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_address) => {
                        if let Ok(mut stream) = TcpStream::connect(socket_address) {
                            let serialized_request = bincode::serialize(&*req);

                            match serialized_request {
                                Ok(ser_req) => {
                                    println!("NODE FAILURE REQUEST SENT: {:?}", req);
                                    stream.write(&ser_req);
                                },
                                Err(_) => {
                                    eprintln!("Error in sending serialized response in send_shard_to_other_replica");
                                }
                            }
                        } else {
                            println!("Failed to TCP send message SEND NODE FAILURE REQUEST");
                        }
                    },
                    Err(e) => {
                        eprintln!("Issue with parsing address in send_shard_to_other_replica");
                    }
                }
            },
            Err(e) => {
                eprintln!("Issue with getting ip address in send_shard_to_other_replica");
            }
        }

        Ok(())
    }

    /**
     * Parses the incoming request from a client 
     */
    fn parse_client_request(&mut self, mut stream: TcpStream) -> Result<()> {
        let mut data = Vec::new();
        let mut buffer = [0; 4096];

        loop {
            let bytes_read = stream.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }

            data.extend_from_slice(&buffer[..bytes_read]);
        }

        println!("BEFORE DESERIALIZATION");
    
        match bincode::deserialize::<ClientRequest>(&data) {
            Ok(ClientRequest::Read(value)) => {
                let val_clone = value.clone(); // Clone to use after move in helper function
                println!("REQ: {:?}", val_clone);
                self.read_request_helper(value); // Assuming value is moved here
                Ok(())
            },
            Ok(ClientRequest::Write(value)) => {
                let val_clone = value.clone(); // Clone to use after move in helper function
                println!("REQ: {:?}", val_clone);
                self.write_request_helper(value); // Assuming value is moved here
                Ok(())
            },
            Ok(ClientRequest::Delete(value)) => {
                let val_clone = value.clone(); // Clone to use after move in helper function
                println!("REQ: {:?}", val_clone);
                self.delete_request_helper(value); // Assuming value is moved here
                Ok(())
            },
            // Add other matches for the different ClientRequest variants if needed
            Err(e) => {
                println!("ERR: {:?}", e);
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Could not deserialize to any client request struct",
                ))
            },
        }
    }

    fn append_request_to_hold_vector(&self, req: ClientRequest) {
        if let Ok(mut holder_vec) = self.hold_requests.lock() {
            holder_vec.push(req);
        }
    }

    fn write_request_helper(&mut self, write_req: WriteRequest) {
        {
            if let Ok(mut metadata_map) = self.filename_to_metadata_map.lock() {
                let file_metadata = metadata_map.entry(write_req.sdfs_filename.clone()).or_insert_with(FileMetadata::new);

                if let (Ok(current_write_bool), Ok(current_reads)) = (file_metadata.currently_writing.lock(), file_metadata.current_reads.lock()) {
                    println!("ENTERRED CURRENTLY WRITING");
                    if *current_write_bool == true || *current_reads >= 1 {
                        self.append_request_to_hold_vector(ClientRequest::Write(write_req.clone()));
                        println!("PUSHED TO HOLD VECTOR: {:?}", write_req);
                        return;
                    }
                }
            }
        }

        if let Ok(mut consec_write) = self.consecutive_write_requests.lock() {
            if *consec_write >= 4 {
                if let Ok(locked_vec) = self.hold_requests.lock() {
                    let request_option = locked_vec.iter()
                        .find(|request| {
                            matches!(request, ClientRequest::Read(_))
                        });

                    match request_option {
                        Some(ClientRequest::Read(read_request)) => {
                            println!("HERRREEEEE1");
                            self.process_read(read_request.clone());
                            *consec_write = 0;

                            let req_clone = write_req.sdfs_filename.clone();

                            println!("iosfhiupfiu");
                            println!("PROCESS PUT LINE 257");
                            self.process_put(write_req);
                            *consec_write += 1;
                        },
                        _ => {
                            println!("ipfijnfjnijn");
                            println!("PROCESS PUT LINE 263");
                            self.process_put(write_req);
                            *consec_write += 1;
                        }
                    }
                }
            } else {
                println!("wuhifwuhiefui");
                let req_clone = write_req.sdfs_filename.clone();
                println!("PROCESS PUT LINE 272");
                self.process_put(write_req);
                *consec_write += 1;
            }
        }
    }

    fn delete_request_helper(&mut self, delete_req: DeleteRequest) {
        {
            if let Ok(mut metadata_map) = self.filename_to_metadata_map.lock() {
                let file_metadata = metadata_map.entry(delete_req.filename.clone()).or_insert_with(FileMetadata::new);

                if let (Ok(current_write_bool), Ok(current_reads)) = (file_metadata.currently_writing.lock(), file_metadata.current_reads.lock()) {
                    if *current_write_bool == true || *current_reads >= 1 {
                        self.append_request_to_hold_vector(ClientRequest::Delete(delete_req.clone()));
                        return;
                    }
                }
            }
        }

        if let Ok(mut curr_write) = self.consecutive_write_requests.lock() {
            if *curr_write >= 4 {
                if let Ok(locked_vec) = self.hold_requests.lock() {
                    let request_option = locked_vec.iter()
                        .find(|request| {
                            matches!(request, ClientRequest::Read(_))
                        });

                    match request_option {
                        Some(ClientRequest::Read(read_request)) => {
                            println!("HERRREEEEE2");
                            self.process_read(read_request.clone());
                            *curr_write = 0;

                            self.process_delete(delete_req);
                            *curr_write += 1;
                        },
                        _ => {
                            self.process_delete(delete_req);
                            *curr_write += 1;
                        }
                    }
                }
            } else {
                self.process_delete(delete_req);
                *curr_write += 1;
            }
        }
    }

    fn read_request_helper(&mut self, read_req: ReadRequest) {
        {
            println!("dsifdui");
            if let Ok(mut metadata_map) = self.filename_to_metadata_map.lock() {
                let file_metadata = metadata_map.entry(read_req.sdfs_filename.clone()).or_insert_with(FileMetadata::new);

                if let (Ok(current_write_bool), Ok(current_reads)) = (file_metadata.currently_writing.lock(), file_metadata.current_reads.lock()) {
                    if *current_write_bool == true || *current_reads >= 2 {
                        self.append_request_to_hold_vector(ClientRequest::Read(read_req.clone()));
                        return;
                    }
                }
            }
        }

        if let Ok(mut curr_read) = self.consecutive_read_requests.lock() {
            if *curr_read >= 4 {
                if let Ok(locked_vec) = self.hold_requests.lock() {
                    let request_option = locked_vec.iter()
                        .find(|request| {
                            matches!(request, ClientRequest::Write(_)) ||
                            matches!(request, ClientRequest::Delete(_))
                        });

                    match request_option {
                        Some(ClientRequest::Write(put_request)) => {
                            println!("iiuiuiuiuuih");
                            println!("PROCESS PUT LINE 350");
                            self.process_put(put_request.clone());
                            *curr_read = 0;

                            println!("HERRREEEEE3");
                            self.process_read(read_req);
                            *curr_read += 1;
                        },
                        Some(ClientRequest::Delete(delete_request)) => {
                            println!("fdisbibb");
                            self.process_delete(delete_request.clone());
                            *curr_read = 0;

                            println!("HERRREEEEE4");
                            self.process_read(read_req);
                            *curr_read += 1;
                        },
                        _ => {
                            println!("HERRREEEEE5");
                            self.process_read(read_req);
                            *curr_read += 1;
                        }
                    }
                }
            } else {
                println!("HERRREEEEE6");
                self.process_read(read_req);
                *curr_read += 1;
            }
        }
    }

    fn process_delete(&self, delete_req: DeleteRequest) {
        if let Ok(mut filename_map) = self.filename_to_locations_map.lock() {
            if let Some(val) = filename_map.get(&delete_req.filename) {
                let rep_list_response = ReplicaListResponse {
                    sdfs_filename: delete_req.filename.clone(),
                    unix_filename: "".to_string(),
                    file_shards: val.clone(),
                    response_type: ResponseType::ReplicaList,
                    request_type_to_send: RequestType::Delete,
                };
    
                println!("uhierwfbiuoeui");
                self.send_replica_list_response(rep_list_response, delete_req.requesting_machine_domain_name);
            } else {
                println!("Key is not in map");
            }
        } else {
            // Handle the error if the lock could not be acquired
            println!("Could not lock the filename_to_locations_map.");
        }
    }
    

    fn process_read(&self, read_req: ReadRequest) {
        match self.filename_to_locations_map.lock() {
            Ok(mut filename_map) => {
                match filename_map.get(&read_req.sdfs_filename) {
                    Some(file_shards) => {
                        let rep_list_response = ReplicaListResponse {
                            sdfs_filename: read_req.sdfs_filename.clone(),
                            unix_filename: read_req.unix_filename.clone(),
                            file_shards: file_shards.clone(),
                            response_type: ResponseType::ReplicaList,
                            request_type_to_send: RequestType::Read,
                        };

                        println!("uerifbhuoebuouu");
    
                        self.send_replica_list_response(rep_list_response, read_req.requesting_machine_domain_name);
                    },
                    None => println!("Key is not in map or an error occurred."),
                }
            },
            Err(e) => {
                println!("Failed to acquire lock on filename_to_locations_map: {:?}", e);
            },
        }
    }

    fn process_put(&self, put_req: WriteRequest) {
        {
            if let Ok(mut metadata_map) = self.filename_to_metadata_map.lock() {
                let file_metadata = metadata_map.entry(put_req.clone().sdfs_filename).or_insert_with(FileMetadata::new);

                if let Ok(mut current_write_bool) = file_metadata.currently_writing.lock() {
                    *current_write_bool = true;
                    println!("BOOLEAN SET TO TRUE for req {:?}", &put_req);
                }
            }
        }

        println!("siduhifuhiouio");
        if let Ok(mut filename_map) = self.filename_to_locations_map.lock() {
            if filename_map.contains_key(&put_req.sdfs_filename) {
                match filename_map.get(&put_req.sdfs_filename) {
                    Some(val) => {
                        let rep_list_response = ReplicaListResponse {
                            sdfs_filename: put_req.sdfs_filename,
                            unix_filename: put_req.unix_filename,
                            file_shards: val.clone(),
                            response_type: ResponseType::ReplicaList,
                            request_type_to_send: RequestType::Write
                        };

                        println!("fibewburobuwbhufrbou");

                        self.send_replica_list_response(rep_list_response, put_req.requesting_machine_domain_name);
                    },
                    None => {
                        println!("Contains Key says it is there but it isn't")
                    }
                }
            } else {
                let mut shards_map = HashMap::new();

                if let Ok(mem_list) = self.membership_list.lock() {
                    let keys: Vec<(String, usize, SystemTime)> = mem_list.keys().cloned().collect();
                    for i in 1..=NUM_SHARDS {
                        let mut rng = rand::thread_rng();
                
                        let mut unique_numbers = HashSet::new();
                
                        while unique_numbers.len() < 4 {
                            // Generate a random number between 1 and the number of keys.
                            let number = rng.gen_range(1..=keys.len());
                            unique_numbers.insert(number);
                        }
                
                        let unique_numbers_vec: Vec<usize> = unique_numbers.into_iter().collect();
                
                        let ip_one = Arc::new(keys[unique_numbers_vec[0] - 1].0.clone());
                        let ip_two = Arc::new(keys[unique_numbers_vec[1] - 1].0.clone());
                        let ip_three = Arc::new(keys[unique_numbers_vec[2] - 1].0.clone());
                        let ip_four = Arc::new(keys[unique_numbers_vec[3] - 1].0.clone());
                
                        let mut domain_one = "".to_string();
                        let mut domain_two = "".to_string();
                        let mut domain_three = "".to_string();
                        let mut domain_four = "".to_string();

                        for elem in DOMAINS {
                            match get_ip_addr(elem) {
                                Ok(val) => {
                                    if val == ip_one {
                                        domain_one = elem.to_string();
                                    }

                                    if val == ip_two {
                                        domain_two = elem.to_string();
                                    }

                                    if val == ip_three {
                                        domain_three = elem.to_string();
                                    }

                                    if val == ip_four {
                                        domain_four = elem.to_string();
                                    }
                                },
                                Err(_) => println!("Issue in assigning domains for four replicas")
                            }
                        }
                        
                        let mut curr_replica_vec = Vec::new();
                        curr_replica_vec.push(domain_one.clone());
                        curr_replica_vec.push(domain_two.clone());
                        curr_replica_vec.push(domain_three.clone());
                        curr_replica_vec.push(domain_four.clone());
    
                        if let Ok(mut machine_shard_map) = self.machine_to_file_shards_map.lock() {
                            if let Some(val) = machine_shard_map.get_mut(&domain_one) {
                                val.push((put_req.sdfs_filename.clone(), i as u16));
                            } else {
                                let mut shard_vec = Vec::new();
                                shard_vec.push((put_req.sdfs_filename.clone(), i as u16));
                                machine_shard_map.insert(domain_one.clone(), shard_vec);
                            }

                            if let Some(val) = machine_shard_map.get_mut(&domain_two) {
                                val.push((put_req.sdfs_filename.clone(), i as u16));
                            } else {
                                let mut shard_vec = Vec::new();
                                shard_vec.push((put_req.sdfs_filename.clone(), i as u16));
                                machine_shard_map.insert(domain_two.clone(), shard_vec);
                            }

                            if let Some(val) = machine_shard_map.get_mut(&domain_three) {
                                val.push((put_req.sdfs_filename.clone(), i as u16));
                            } else {
                                let mut shard_vec = Vec::new();
                                shard_vec.push((put_req.sdfs_filename.clone(), i as u16));
                                machine_shard_map.insert(domain_three.clone(), shard_vec);
                            }

                            if let Some(val) = machine_shard_map.get_mut(&domain_four) {
                                val.push((put_req.sdfs_filename.clone(), i as u16));
                            } else {
                                let mut shard_vec = Vec::new();
                                shard_vec.push((put_req.sdfs_filename.clone(), i as u16));
                                machine_shard_map.insert(domain_four.clone(), shard_vec);
                            }
                        }
    
                        shards_map.insert(i as u16, curr_replica_vec);
                    }
                    
                    filename_map.insert(put_req.sdfs_filename.clone(), shards_map.clone());
    
                    let rep_list_response = ReplicaListResponse {
                        sdfs_filename: put_req.sdfs_filename,
                        unix_filename: put_req.unix_filename,
                        file_shards: shards_map,
                        response_type: ResponseType::ReplicaList,
                        request_type_to_send: RequestType::Write
                    };
            
                    // println!("buiasdububo");
                    self.send_replica_list_response(rep_list_response, put_req.requesting_machine_domain_name);
                }
            }
        }
    }

    /**
     * Sends the replica list for a response
     */
    fn send_replica_list_response(&self, res: ReplicaListResponse, client_domain_name: String) -> Result<()> {
        // println!("UIDIDSHIU");
        let ip_addr = get_ip_addr(&client_domain_name);
        println!("REPLISTRESP INSIDE LEADER: {:?}", res);

        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:8018", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_address) => {
                        if let Ok(mut stream) = TcpStream::connect(socket_address) {
                            let serialized_response = bincode::serialize(&res);

                            match serialized_response {
                                Ok(ser_res) => {
                                    println!("SENT REPLICA LIST RESPONSE FROM LEADER");
                                    stream.write(&ser_res);
                                },
                                Err(_) => {
                                    eprintln!("Error in sending serialized response in send_write_response_to_client");
                                }
                            }
                        } else {
                            println!("Failed to TCP send message SEND REPLICA LIST RESPONSE");
                        }
                    },
                    Err(e) => {
                        eprintln!("Issue with parsing address in send_write_response_to_client");
                    }
                }
            },
            Err(e) => {
                eprintln!("Issue with getting ip address in send_write_response_to_client");
            }
        }

        Ok(())
    }

    fn listen_to_client_requests(&mut self) -> Result<()> {
        println!("dsdfididsi");
        let ip_addr = get_ip_addr(&self.machine_domain_name);

        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:8017", ip.to_string());
                println!("ADD LISTEN LEADER CLIENT REQ: {}", address_str);
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_addr) => {
                        let listener = TcpListener::bind(socket_addr)?;
                        println!("ADD LISTEN LEADER CLIENT REQ: {:?}", socket_addr);
                        for stream in listener.incoming() {
                            match stream {
                                Ok(stream) => {
                                    println!("GOT CLIENT REQUEST");
                                    self.parse_client_request(stream);
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

    fn parse_client_ack(&self, mut stream: TcpStream) -> Result<()> {
        // println!("iuhifdidsji");
        let mut data = Vec::new();
        let mut buffer = [0; 4096];

        loop {
            let bytes_read = stream.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }

            data.extend_from_slice(&buffer[..bytes_read]);
        }
    
        if let Ok(value) = bincode::deserialize::<RequestAck>(&data) {
            // println!("nfsjdiohfuisdobfuiso");
            self.handle_client_acks(value);
            return Ok(());
        } else {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Could not deserialize to the client Ack struct",
            ));
        }
    }

    fn handle_read_ack(&self, req_ack: RequestAck) {
        // println!("ijfosdbuiob");
        if let Ok(mut file_data_map) = self.filename_to_metadata_map.lock() {
            if file_data_map.contains_key(&req_ack.filename) {
                match file_data_map.get_mut(&req_ack.filename) {
                    Some(val) => {
                        {
                            if let Ok(mut curr_reads) = val.current_reads.lock() {
                                *curr_reads -= 1;

                                if *curr_reads == 1 {
                                    {
                                        if let Ok(locked_vec) = self.hold_requests.lock() {
                                            let request_option = locked_vec.iter()
                                                .find(|request| {
                                                    if let ClientRequest::Read(read_request) = request {
                                                        read_request.sdfs_filename == req_ack.filename
                                                    } else {
                                                        false
                                                    }
                                                });
                        
                                            match request_option {
                                                Some(ClientRequest::Read(read_request)) => {
                                                    println!("HERRREEEEE7");
                                                    self.process_read(read_request.clone());
                                                    if let Ok(mut consec_reads) = self.consecutive_read_requests.lock() {
                                                        *consec_reads += 1;
                                                    }

                                                    *curr_reads = 2;
                                                },
                                                _ => {
                                                    //DO NOTHING
                                                }
                                            }
                                        }
                                    }
                                } else if *curr_reads == 0 {
                                    {
                                        if let Ok(locked_vec) = self.hold_requests.lock() {
                                            let request_option = locked_vec.iter()
                                                .find(|request| {
                                                    if let ClientRequest::Write(put_request) = request {
                                                        put_request.sdfs_filename == req_ack.filename
                                                    } else if let ClientRequest::Delete(delete_request) = request {
                                                        delete_request.filename == req_ack.filename
                                                    } else {
                                                        false
                                                    }
                                                });
                        
                                            match request_option {
                                                Some(ClientRequest::Write(put_request)) => {
                                                    println!("PROCESS PUT LINE 705");
                                                    self.process_put(put_request.clone());

                                                    if let Ok(mut consec_writes) = self.consecutive_write_requests.lock() {
                                                        *consec_writes += 1;
                                                    }
                                                },
                                                Some(ClientRequest::Delete(delete_request)) => {
                                                    self.process_delete(delete_request.clone());
                                                    if let Ok(mut consec_writes) = self.consecutive_write_requests.lock() {
                                                        *consec_writes += 1;
                                                    }
                                                },
                                                _ => {
                                                    //DO NOTHING
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    },
                    None => {
                        println!("Contains Key says it is there but it isn't")
                    }
                }
            } else {
                println!("Key is not in map");
            }
        }
    }

    fn handle_write_ack(&self, req_ack: RequestAck) {
        // println!("njdfisobfubsduofboui");
        if let Ok(mut file_data_map) = self.filename_to_metadata_map.lock() {
            if file_data_map.contains_key(&req_ack.filename) {
                match file_data_map.get_mut(&req_ack.filename) {
                    Some(val) => {
                        {
                            if let Ok(mut locked_write_bool) = val.currently_writing.lock() {
                                *locked_write_bool = false;
                            }

                            if let Ok(locked_vec) = self.hold_requests.lock() {
                                let request_option = locked_vec.iter()
                                    .find(|request| {
                                        if let ClientRequest::Read(read_request) = request {
                                            read_request.sdfs_filename == req_ack.filename
                                        } else if let ClientRequest::Write(write_request) = request {
                                            write_request.sdfs_filename == req_ack.filename
                                        } else if let ClientRequest::Delete(delete_request) = request {
                                            delete_request.filename == req_ack.filename
                                        } else {
                                            false
                                        }
                                    });
            
                                match request_option {
                                    Some(ClientRequest::Read(read_request)) => {
                                        println!("HERRREEEEE9");
                                        self.process_read(read_request.clone());
                                        if let Ok(mut consec_reads) = self.consecutive_read_requests.lock() {
                                            *consec_reads += 1;
                                        }
                                    },
                                    Some(ClientRequest::Write(put_request)) => {
                                        println!("PROCESS PUT LINE 772: {:?}", put_request);
                                        self.process_put(put_request.clone());

                                        if let Ok(mut consec_writes) = self.consecutive_write_requests.lock() {
                                            *consec_writes += 1;
                                        }
                                    },
                                    Some(ClientRequest::Delete(delete_request)) => {
                                        self.process_delete(delete_request.clone());
                                        if let Ok(mut consec_writes) = self.consecutive_write_requests.lock() {
                                            *consec_writes += 1;
                                        }
                                    },
                                    _ => {
                                        //DO NOTHING
                                    }
                                }
                            }
                        }
                    },
                    None => {
                        println!("Contains Key says it is there but it isn't")
                    }
                }
            } else {
                println!("Key is not in map");
            }
        }
    }

    fn handle_client_acks(&self, req_ack: RequestAck) {
        // println!("jskaflbjhdsbfhu");
        match req_ack.ack_type {
            AckType::Read => {
                println!("LEADER SDFS RECEIVED READ ACK");
                self.handle_read_ack(req_ack);
            },
            AckType::Write => {
                println!("LEADER SDFS RECEIVED WRITE ACK");
                self.handle_write_ack(req_ack);
            },
            AckType::Delete => {
                if let Ok(mut map) = self.filename_to_metadata_map.lock() {
                    map.remove(&req_ack.filename);
                }

                if let Ok(mut map) = self.filename_to_locations_map.lock() {
                    map.remove(&req_ack.filename);
                }

                if let Ok(mut map) = self.machine_to_file_shards_map.lock() {
                    for (_, value) in map.iter_mut() {
                        value.retain(|(filename, _)| filename != &req_ack.filename);
                    }
                }
            },
            _ => {
                println!("No other acks should come at this port");
            }
        }
    }

    fn listen_to_client_acks(&self) -> Result<()>{
        // println!("njkdsjnkdjkndsjdfsjb");
        let ip_addr = get_ip_addr(&self.machine_domain_name);

        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:8019", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_addr) => {
                        let listener = TcpListener::bind(socket_addr)?;

                        for stream in listener.incoming() {
                            match stream {
                                Ok(stream) => {
                                    println!("RECEIVED CLIENT ACK");
                                    self.parse_client_ack(stream);
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

    fn parse_ls_request(&self, mut stream: TcpStream) -> Result<()> {
        // println!("ndsufiondfsuiobif");
        let mut data = Vec::new();
        let mut buffer = [0; 4096];

        loop {
            let bytes_read = stream.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }

            data.extend_from_slice(&buffer[..bytes_read]);
        }
    
        if let Ok(value) = bincode::deserialize::<LsRequest>(&data) {
            if let Ok(map) = self.filename_to_locations_map.lock() {
                match map.get(&value.filename) {
                    Some(val) => {
                        let ls_response = LsResponse {
                            filename: value.filename,
                            file_shards: val.clone(),
                        };
        
                        self.send_ls_response(ls_response, value.requesting_machine_domain_name);
                    }, None => {
                        println!("File that was LSed was not in the map");

                        let ls_response = LsResponse {
                            filename: value.filename,
                            file_shards: HashMap::new(),
                        };
        
                        self.send_ls_response(ls_response, value.requesting_machine_domain_name);
                    }
                }
            }
        } else {
            println!("Could not deserialize to an LsRequest");
        }

        return Ok(())
    }

    fn send_ls_response(&self, ls_res: LsResponse, client_domain_name: String) -> Result<()> {
        // println!("dfsjbfhuosdbfhuobsfhu");
        let ip_addr = get_ip_addr(&client_domain_name);

        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:8021", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_address) => {
                        if let Ok(mut stream) = TcpStream::connect(socket_address) {
                            let serialized_response = bincode::serialize(&ls_res);

                            match serialized_response {
                                Ok(ser_res) => {
                                    println!("SEND LS RESPONSE BACK");
                                    stream.write(&ser_res);
                                },
                                Err(_) => {
                                    eprintln!("Error in sending serialized response in send_write_response_to_client");
                                }
                            }
                        } else {
                            println!("Failed to TCP send message SEND LS RESPONSE");
                        }
                    },
                    Err(e) => {
                        eprintln!("Issue with parsing address in send_write_response_to_client");
                    }
                }
            },
            Err(e) => {
                eprintln!("Issue with getting ip address in send_write_response_to_client");
            }
        }

        Ok(())
    } 

    fn listen_to_client_ls_commands(&self) -> Result<()> {
        // println!("fndjsiobfuisobfhudsobfhuo");
        let ip_addr = get_ip_addr(&self.machine_domain_name);

        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:8020", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_addr) => {
                        let listener = TcpListener::bind(socket_addr)?;

                        for stream in listener.incoming() {
                            match stream {
                                Ok(stream) => {
                                    println!("RECEIVED CLIENT LS REQUEST");
                                    self.parse_ls_request(stream);
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

    fn parse_ack(&self, mut stream: TcpStream) -> Result<()> {
        println!("ENTER PARSE ACK");
        let mut data = Vec::new();
        let mut buffer = [0; 4096];

        loop {
            let bytes_read = stream.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }

            data.extend_from_slice(&buffer[..bytes_read]);
        }
    
        if let Ok(value) = bincode::deserialize::<RequestAck>(&data) {
            println!("ACK GOT BACK: {:?}", value);
            self.handle_node_failure_ack(value);
            return Ok(());
        } else {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Could not deserialize to the client Ack struct",
            ));
        }
    }

    fn handle_node_failure_ack(&self, ack: RequestAck) {
        // println!("njdksbfnjsdbfhsdbhf");
        match ack.ack_type {
            AckType::NodeFailure => {
                if let Ok(mut map2) = self.filename_to_locations_map.lock() {
                    if let Some(shards_map) = map2.get_mut(&ack.filename) {
                        if let Some(machines_containing_shard) = shards_map.get_mut(&ack.shard_num) {
                            println!("File {} was properly rereplicated!", ack.filename);
                            machines_containing_shard.push(ack.new_machine_containing_shard);
                        }
                    }
                }

                println!("No other acks other than Node Failure should come at this port");
            }, 
            _ => {
                println!("WRONG TYPE OF ACK AT THIS PORT");
            }
        }
    }

    fn listen_for_node_failure_acks(&self) -> Result<()> {
        // println!("sjknfjnkdjnkdfnjk");
        let ip_addr = get_ip_addr(&self.machine_domain_name);

        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:8016", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_addr) => {
                        let listener = TcpListener::bind(socket_addr)?;

                        for stream in listener.incoming() {
                            match stream {
                                Ok(stream) => {
                                    println!("GOT NODE FAILURE ACK FROM REPLICA");
                                    self.parse_ack(stream);
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
    /**
     * Main function to start functionality for the Leader Listener 
     */
    pub fn start_listening(&mut self) {
        let client_connection_join_handle = Arc::new(Mutex::new(None));
        let client_ack_join_handle = Arc::new(Mutex::new(None));
        let client_ls_join_handle = Arc::new(Mutex::new(None));
        let replica_ack_join_handle = Arc::new(Mutex::new(None));

        let mut self_clone = self.clone();


        let client_handle_storage = Arc::clone(&client_connection_join_handle);
        let ack_handle_storage = Arc::clone(&client_ack_join_handle);
        let ls_handle_storage = Arc::clone(&client_ls_join_handle);
        let replica_handle_storage = Arc::clone(&replica_ack_join_handle);

        let clients_thread_handle = thread::spawn(move || {
            if let Err(e) = self_clone.listen_to_client_requests() {
                eprintln!("Error reading from suspicion stream: {}", e);
            }
        });

        let self_clone = self.clone();

        let ack_thread_handle = thread::spawn(move || {
            self_clone.listen_to_client_acks().unwrap();
        });

        let self_clone = self.clone();

        let ls_thread_handle = thread::spawn(move || {
            self_clone.listen_to_client_ls_commands().unwrap(); 
        });

        let self_clone = self.clone();

        let replica_thread_handle = thread::spawn(move || {
            self_clone.listen_for_node_failure_acks().unwrap();
        });

        let mut handle = client_handle_storage.lock().unwrap();
        *handle = Some(clients_thread_handle);

        let mut handle = ack_handle_storage.lock().unwrap();
        *handle = Some(ack_thread_handle);

        let mut handle = ls_handle_storage.lock().unwrap();
        *handle = Some(ls_thread_handle);

        let mut handle = replica_handle_storage.lock().unwrap();
        *handle = Some(replica_thread_handle);

    }

    pub fn close_threads(&mut self) {
        if let Ok(mut handle) = self.client_connection_join_handle.lock() {
            if let Some(hdl) = handle.take() {
                hdl.join().expect("Client connection thread in Replica panicked!");
            }
        }

        if let Ok(mut handle) = self.client_ack_join_handle.lock() {
            if let Some(hdl) = handle.take() {
                hdl.join().expect("Leader connection thread in Replica panicked!");
            }
        }

        if let Ok(mut handle) = self.client_ls_join_handle.lock() {
            if let Some(hdl) = handle.take() {
                hdl.join().expect("Replica connection One thread in Replica panicked!");
            }
        }

        if let Ok(mut handle) = self.replica_ack_join_handle.lock() {
            if let Some(hdl) = handle.take() {
                hdl.join().expect("Replica connection Two connection thread in Replica panicked!");
            }
        }
    }
}