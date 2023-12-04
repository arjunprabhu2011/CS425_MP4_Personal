use std::fs::{File, self};
use std::io::{Write, self, Read};
use std::thread;
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::io::Result;
use std::sync::{Arc, Mutex};
use std::fs::OpenOptions;
use bincode;
use std::path::Path;
use crate::utils::*;

#[derive(Clone)]
pub struct ReplicaListener {
    client_connection_join_handle: Arc<Mutex<Option<thread::JoinHandle<()>>>>, //This handle used for when we want to join the threads
    leader_connection_join_handle: Arc<Mutex<Option<thread::JoinHandle<()>>>>, //This handle used for when we want to join the threads
    replica_connection_request_join_handle: Arc<Mutex<Option<thread::JoinHandle<()>>>>, //This handle used for when we want to join the threads
    replica_connection_response_join_handle: Arc<Mutex<Option<thread::JoinHandle<()>>>>, //This handle used for when we want to join the threads
    machine_domain_name: Arc<String>,
    current_files: Arc<Mutex<Vec<(String, u16)>>>,
    leader_domain_name: Arc<Mutex<String>>
}

/**
 * Main strcuture for replicas in sdfs
 */
impl ReplicaListener {
    pub fn new(machine_name: Arc<String>, leader_name: Arc<Mutex<String>>, curr_files: Arc<Mutex<Vec<(String, u16)>>>) -> Self {
        ReplicaListener { 
            client_connection_join_handle: Arc::new(Mutex::new(None)), 
            leader_connection_join_handle: Arc::new(Mutex::new(None)), 
            replica_connection_request_join_handle: Arc::new(Mutex::new(None)), 
            replica_connection_response_join_handle: Arc::new(Mutex::new(None)), 
            machine_domain_name: machine_name, 
            current_files: curr_files,
            leader_domain_name: leader_name
        } 
    }

    pub fn print_current_files(&self) {
        if let Ok(files) = self.current_files.lock() {
            for (filename, shard) in files.iter() {
                println!("Filename: {}, Shard: {}", filename, shard);
            }
        }
    }

    fn update_leader_domain_name(&mut self, new_name: &str) {
        if let Ok(mut leader_name) = self.leader_domain_name.lock() {
            *leader_name = new_name.to_string();
        }
    }

    fn insert_current_files_vec(&self, filename: String, shard: u16) {
        if let Ok(mut curr_files) = self.current_files.lock() {
            let filename_clone = filename.clone();
            if !curr_files.contains(&(filename, shard)) {
                curr_files.push((filename_clone, shard));
            }
        }
    }

    fn delete_from_current_files_vec(&self, filename: String, shard: u16) {
        if let Ok(mut curr_files) = self.current_files.lock() {
            let filename_clone = filename.clone(); // Clone `filename` here
            curr_files.retain(|&(ref f, s)| f != &filename_clone || s != shard);
        }
    }
    

    fn listen_to_node_failure_responses(&self) -> Result<()> {
        let ip_addr = get_ip_addr(&self.machine_domain_name);

        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:8015", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_addr) => {
                        let listener = TcpListener::bind(socket_addr)?;

                        for stream in listener.incoming() {
                            match stream {
                                Ok(stream) => {
                                    self.parse_node_failure_response_from_replica(stream);
                                },
                                Err(e) => {
                                    eprintln!("Connection failed in listen_to_node_failure_responses: {}", e);
                                }
                            }
                        }
                    },
                    Err(e) => {
                        eprintln!("Address parse error in listen_to_node_failure_responses: {}", e);
                    }
                }
            }, 
            Err(e) => {
                eprintln!("Get ip address error in listen_to_node_failure_responses: {}", e);
            }
        };

        Ok(())
    }

    fn parse_node_failure_response_from_replica(&self, mut stream: TcpStream) -> Result<()> {
        let mut data = Vec::new();
        let mut buffer = [0; 4096];

        loop {
            let bytes_read = stream.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }

            data.extend_from_slice(&buffer[..bytes_read]);
        }

        println!("ENTER PARSE NODE FAILURE RESPONSE");
    
        if let Ok(value) = bincode::deserialize::<NodeFailureResponse>(&data) {
            println!("NODE FAILURE RESPONSE ON RECEIVING END: {:?}", value);
            self.send_ack_to_leader(value);
            return Ok(());
        } else {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Could not deserialize to Node Failure Response",
            ));
        }
    }

    fn send_ack_to_leader(&self, res: NodeFailureResponse) -> Result<()> {
        let node_failure_ack = RequestAck {
            ack_type: AckType::NodeFailure,
            filename: res.filename,
            machine_domain_name: self.machine_domain_name.as_ref().clone(),
            new_machine_containing_shard: res.new_machine_holding_shard,
            shard_num: res.shard_number
        };
        
        if let Ok(leader_name) = self.leader_domain_name.lock() {
            let ip_addr = get_ip_addr(&leader_name);

            match ip_addr {
                Ok(ip) => {
                    let address_str = format!("{}:8016", ip.to_string());
                    match address_str.parse::<SocketAddr>() {
                        Ok(socket_address) => {
                            if let Ok(mut stream) = TcpStream::connect(socket_address) {
                                let serialized_ack = bincode::serialize(&node_failure_ack);

                                match serialized_ack {
                                    Ok(ser_ack) => {
                                        println!("SENT ACK FOR NODE FAILURE BACK TO LEADER (REPLICA NODE)");
                                        stream.write(&ser_ack);
                                    },
                                    Err(_) => {
                                        eprintln!("Error in sending serialized response in send_ack_to_leader");
                                    }
                                }
                            } else {
                                println!("Failed to TCP send message SEND ACK TO LEADER SDFS");
                            }
                        },
                        Err(e) => {
                            eprintln!("Issue with parsing address in send_ack_to_leader");
                        }
                    }
                },
                Err(e) => {
                    eprintln!("Issue with getting ip address in send_ack_to_leader");
                }
            }
        }

        Ok(())
    }

    fn listen_to_write_requests_from_other_replicas(&mut self) -> Result<()> {
        let ip_addr = get_ip_addr(&self.machine_domain_name);

        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:8014", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_addr) => {
                        let listener = TcpListener::bind(socket_addr)?;

                        for stream in listener.incoming() {
                            match stream {
                                Ok(stream) => {
                                    println!("RECEIVED WRITE REQUEST FROM REPLICA (REPLICA NODE)");
                                    self.parse_write_request_from_replica(stream);
                                },
                                Err(e) => {
                                    eprintln!("Connection failed in listen_to_write_requests_from_other_replicas: {}", e);
                                }
                            }
                        }
                    },
                    Err(e) => {
                        eprintln!("Address parse error in listen_to_write_requests_from_other_replicas: {}", e);
                    }
                }
            }, 
            Err(e) => {
                eprintln!("Get ip address error in listen_to_write_requests_from_other_replicas: {}", e);
            }
        };

        Ok(())
    }

    fn parse_write_request_from_replica(&mut self, mut stream: TcpStream) -> Result<()> {
        let mut data = Vec::new();
        let mut buffer = [0; 4096];

        loop {
            let bytes_read = stream.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }

            data.extend_from_slice(&buffer[..bytes_read]);
        }

        println!("PARSING WRITE REQUEST FROM REPLICA");
    
        match bincode::deserialize::<ClientRequest>(&data) {
            Ok(ClientRequest::Write(value)) => {
                let file_name = value.sdfs_filename.clone();
                let arc_file_name = Arc::new(file_name);
                let path = Path::new(&*arc_file_name);
                if !path.exists() {
                    self.write_request_helper_upon_node_failure(value);
                }
                Ok(())
            },
            // Handle other request variants if necessary here
            Ok(_) => {
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Deserialized to a different type of client request",
                ))
            },
            Err(_) => {
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Could not deserialize to any client request type",
                ))
            },
        }
        
    }

    fn write_request_helper_upon_node_failure(&mut self, mut req: WriteRequest) -> Result<()> {
        // Clone the filename before moving it into the OpenOptions
        let filename_clone = req.sdfs_filename.clone();
    
        let mut file = OpenOptions::new()
                            .write(true)
                            .create(true)
                            .truncate(true)
                            .open(&filename_clone)?;  // Use a reference to the cloned filename
    
        file.write_all(&req.data)?;

        println!("WRITE TO WRITE: {:?}", req);
        println!("WROTE TO FILE");
    
        // Since we cloned the filename, we can still use req.sdfs_filename here
        self.insert_current_files_vec(filename_clone.clone(), req.shard_number);
    
        // For the response, you can clone again if needed
        let node_failure_response = NodeFailureResponse {
            filename: req.sdfs_filename,  // This will move the filename, so it can't be used afterwards
            shard_number: req.shard_number,
            new_machine_holding_shard: (*self.machine_domain_name).clone(),  // Clone if machine_domain_name is not Copy
            response_type: ResponseType::NodeFailure
        };
    
        self.send_node_failure_response_to_replica(node_failure_response, req.requesting_machine_domain_name.clone());  // Clone if necessary
    
        Ok(())
    }
    
    fn send_node_failure_response_to_replica(&self, res: NodeFailureResponse, client_domain_name: String) -> Result<()> {
        let ip_addr = get_ip_addr(&client_domain_name);

        println!("NODE FAILURE RESPONSE BACK TO ORIGINAL REPLICA: {:?}", res);

        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:8015", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_address) => {
                        if let Ok(mut stream) = TcpStream::connect(socket_address) {
                            let serialized_response = bincode::serialize(&res);

                            match serialized_response {
                                Ok(ser_res) => {
                                    println!("SENT NODE FAILURE RESPONSE BACK TO REPLICA (REPLICA NODE)");
                                    stream.write(&ser_res);
                                },
                                Err(_) => {
                                    eprintln!("Error in sending serialized response in send_node_failure_response_to_replica");
                                }
                            }
                        } else {
                            println!("Failed to TCP send message SEND NODE FAILURE RESPONSE TO OTHER REPLICA");
                        }
                    },
                    Err(e) => {
                        eprintln!("Issue with parsing address in send_node_failure_response_to_replica");
                    }
                }
            },
            Err(e) => {
                eprintln!("Issue with getting ip address in send_node_failure_response_to_replica");
            }
        }

        Ok(())
    }

    fn listen_to_node_failure_requests(&self) -> Result<()> {
        let ip_addr = get_ip_addr(&self.machine_domain_name);

        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:8013", ip.to_string());
                    match address_str.parse::<SocketAddr>() {
                        Ok(socket_addr) => {
                            let listener = TcpListener::bind(socket_addr)?;
    
                            for stream in listener.incoming() {
                                match stream {
                                    Ok(stream) => {
                                        println!("GOT NODE FAILURE REQUEST FROM LEADER (REPLICA NODE)");
                                        self.parse_node_failure_request(stream);
                                    },
                                    Err(e) => {
                                        eprintln!("Connection failed in listen_to_node_failure_requests: {}", e);
                                    }
                                }
                            }
                        },
                        Err(e) => {
                            eprintln!("Address parse error in listen_to_node_failure_requests: {}", e);
                        }
                    }
            }, 
            Err(e) => {
                eprintln!("Get ip address error in listen_to_node_failure_requests: {}", e);
            }
        };

        Ok(())
    }

    fn send_shard_to_other_replica(&self, req: NodeFailureRequest) -> Result<()> {
        let filename_clone = req.filename.clone();
        let mut file = File::open(req.filename)?;

        let mut file_data = Vec::new();

        file.read_to_end(&mut file_data)?;

        let write_req = WriteRequest {
            sdfs_filename: filename_clone,
            unix_filename: "".to_string(),
            requesting_machine_domain_name: self.machine_domain_name.as_ref().clone(),
            data: file_data,
            shard_number: req.shard_number,
            request_type: RequestType::Write
        };

        println!("WRITE REQ TO SEND 362: {:?}", write_req);

        let ip_addr = get_ip_addr(&req.machine_domain_name_to_send_shard_to);

        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:8014", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_address) => {
                        if let Ok(mut stream) = TcpStream::connect(socket_address) {
                            let req = ClientRequest::Write(write_req);
                            let serialized_request = bincode::serialize(&req);

                            match serialized_request {
                                Ok(ser_req) => {
                                    println!("SENT WRITE REQUEST TO OTHER REPLICA (REPLICA NODE)");
                                    stream.write(&ser_req);
                                },
                                Err(_) => {
                                    eprintln!("Error in sending serialized response in send_shard_to_other_replica");
                                }
                            }
                        } else {
                            println!("Failed to TCP send message SEND SHARD TO OTHER REPLICA");
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

    fn parse_node_failure_request(&self, mut stream: TcpStream) -> Result<()> {
        let mut data = Vec::new();
        let mut buffer = [0; 4096];

        loop {
            let bytes_read = stream.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }

            data.extend_from_slice(&buffer[..bytes_read]);
        }

        //SEND NODEFAILUREREQUEST TO OTHER REPLICA
    
        if let Ok(value) = bincode::deserialize::<NodeFailureRequest>(&data) {
            println!("NODE FAIL REQUEST 415: {:?}", value);
            self.send_shard_to_other_replica(value);

            return Ok(());
        } else {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Could not deserialize to Node Failure Request",
            ));
        }
    }

    fn listen_to_client_requests(&self) -> Result<()> {
        let ip_addr = get_ip_addr(&self.machine_domain_name);

        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:8011", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_addr) => {
                        let listener = TcpListener::bind(socket_addr)?;

                        for stream in listener.incoming() {
                            match stream {
                                Ok(stream) => {
                                    println!("GOT A CLIENT REQUEST (REPLICA NODE)");
                                    let mut self_clone = self.clone();
                                    thread::spawn(move || {
                                        self_clone.parse_client_request(stream);
                                    });
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
    
        match bincode::deserialize::<ClientRequest>(&data) {
            Ok(ClientRequest::Read(value)) => {
                println!("RECEIVED READ REQUEST");
                self.read_request_helper(value);
                Ok(())
            },
            Ok(ClientRequest::Write(value)) => {
                println!("RECEIVED WRITE REQUEST");
                self.write_request_helper(value);
                Ok(())
            },
            Ok(ClientRequest::Delete(value)) => {
                println!("RECEIVED DELETE REQUEST");
                self.delete_request_helper(value);
                Ok(())
            },
            // You can add other arms for different request types if necessary
            Err(_) => {
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Could not deserialize to any client request struct",
                ))
            },
        }
    }

    fn read_request_helper(&self, req: ReadRequest) -> io::Result<Vec<u8>> {
        // Use the cloned filename to open the file.
        let filename_clone = req.sdfs_filename.clone();
        let mut file = File::open(&filename_clone)?;
    
        // Read the file data.
        let mut file_data = Vec::new();
        file.read_to_end(&mut file_data)?;
    
        // Clone file_data since it needs to be used after moving it into ReadResponse.
        let data_clone = file_data.clone();
    
        let read_response = ReadResponse {
            filename: filename_clone,
            unix_filename: req.unix_filename,
            data: data_clone,
            shard_number: req.shard_number,
            responding_machine_domain_name: self.machine_domain_name.as_ref().clone(),
            response_type: ResponseType::Read
        };
    
        // Send the read response to the client.
        self.send_read_response_to_client(read_response, req.requesting_machine_domain_name);
    
        // Return the original file_data.
        Ok(file_data)
    }
    

    fn send_read_response_to_client(&self, res: ReadResponse, client_domain_name: String) -> Result<()> {
        let ip_addr = get_ip_addr(&client_domain_name);

        match ip_addr.clone() {
            Ok(ip) => {
                let address_str = format!("{}:8012", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_address) => {
                        if let Ok(mut stream) = TcpStream::connect(socket_address) {
                            let response_wrapper = ResponseWrapper::Read(res.clone());
                            let serialized_response = bincode::serialize(&response_wrapper);

                            match serialized_response {
                                Ok(ser_res) => {
                                    println!("SENT READ RESPONSE BACK TO CLIENT (REPLICA NODE): {}", address_str);
                                    stream.write(&ser_res);
                                },
                                Err(_) => {
                                    eprintln!("Error in sending serialized response in send_read_response_to_client");
                                }
                            }
                        } else {
                            println!("Failed to TCP send message SEND READ RESPONSE TO CLIENT ONE");
                        }
                    },
                    Err(e) => {
                        eprintln!("Issue with parsing address in send_read_response_to_client");
                    }
                }
            },
            Err(e) => {
                eprintln!("Issue with getting ip address in send_read_response_to_client");
            }
        }

        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:8025", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_address) => {
                        if let Ok(mut stream) = TcpStream::connect(socket_address) {
                            let response_wrapper = ResponseWrapper::Read(res);
                            let serialized_response = bincode::serialize(&response_wrapper);

                            match serialized_response {
                                Ok(ser_res) => {
                                    println!("SENT READ RESPONSE BACK TO BLOCKING GET (REPLICA NODE)");
                                    stream.write(&ser_res);
                                },
                                Err(_) => {
                                    eprintln!("Error in sending serialized response in send_read_response_to_client");
                                }
                            }
                        } else {
                            println!("Failed to TCP send message SEND READ RESPONSE TO CLIENT TWO");
                        }
                    },
                    Err(e) => {
                        eprintln!("Issue with parsing address in send_read_response_to_client");
                    }
                }
            },
            Err(e) => {
                eprintln!("Issue with getting ip address in send_read_response_to_client");
            }
        }

        Ok(())
    }

    fn write_request_helper(&mut self, req: WriteRequest) -> Result<()> {
        // Clone the filename so that we can use it after moving it into the OpenOptions.
        let filename_clone = req.sdfs_filename.clone();
    
        // Open the file for writing.
        let mut file = OpenOptions::new()
                            .write(true)
                            .create(true)
                            .truncate(true)
                            .open(&filename_clone)?;
    
        // Write the data to the file.
        file.write_all(&req.data)?;
    
        // Insert the current file and shard number into the tracking vector.
        // You must clone the filename again since `insert_current_files_vec` likely needs to own the string.
        self.insert_current_files_vec(filename_clone.clone(), req.shard_number);
    
        // Prepare the write response.
        let write_response = WriteResponse {
            filename: filename_clone, // Use the cloned filename here.
            shard_number: req.shard_number,
            responding_machine_domain_name: self.machine_domain_name.as_ref().clone(),
            response_type: ResponseType::Write
        };
    
        // Send the write response to the client.
        self.send_write_response_to_client(write_response, req.requesting_machine_domain_name);
    
        Ok(())
    }
    

    fn send_write_response_to_client(&self, res: WriteResponse, client_domain_name: String) -> Result<()> {
        let ip_addr = get_ip_addr(&client_domain_name);

        match ip_addr.clone() {
            Ok(ip) => {
                let address_str = format!("{}:8012", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_address) => {
                        if let Ok(mut stream) = TcpStream::connect(socket_address) {
                            let response_wrapper = ResponseWrapper::Write(res.clone());
                            let serialized_response = bincode::serialize(&response_wrapper);

                            match serialized_response {
                                Ok(ser_res) => {
                                    println!("SENT WRITE RESPONSE BACK TO CLIENT (REPLICA NODE)");
                                    stream.write(&ser_res);
                                },
                                Err(_) => {
                                    eprintln!("Error in sending serialized response in send_write_response_to_client");
                                }
                            }
                        } else {
                            println!("Failed to TCP send message SEND WRITE RESPONSE TO CLIENT ONE");
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

        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:8025", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_address) => {
                        if let Ok(mut stream) = TcpStream::connect(socket_address) {
                            let response_wrapper = ResponseWrapper::Write(res);
                            let serialized_response = bincode::serialize(&response_wrapper);

                            match serialized_response {
                                Ok(ser_res) => {
                                    println!("SENT WRITE RESPONSE BACK TO BLOCKING PUT (REPLICA NODE): {}", address_str);
                                    stream.write(&ser_res);
                                },
                                Err(_) => {
                                    eprintln!("Error in sending serialized response in send_write_response_to_client");
                                }
                            }
                        } else {
                            println!("Failed to TCP send message SEND WRITE RESPONSE TO CLIENT TWO");
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

    fn delete_request_helper(&mut self, req: DeleteRequest) -> Result<()>{
        let filename_clone = req.filename.clone();
    
        match fs::remove_file(&filename_clone) {
            Ok(_) => println!("File {} deleted successfully.", &filename_clone),
            Err(e) => println!("Error deleting file: {:?}", e),
        }
    
        self.delete_from_current_files_vec(filename_clone.clone(), req.shard_number);
    
        // Prepare the delete response.
        let delete_response = DeleteResponse {
            filename: filename_clone, // Use the cloned filename here.
            shard_number: req.shard_number,
            responding_machine_domain_name: self.machine_domain_name.as_ref().clone(),
            response_type: ResponseType::Delete
        };
    
        // Send the delete response to the client.
        self.send_delete_response_to_client(delete_response, req.requesting_machine_domain_name);
    
        Ok(())
    }
    

    fn send_delete_response_to_client(&self, res: DeleteResponse, client_domain_name: String) -> Result<()> {
        let ip_addr = get_ip_addr(&client_domain_name);

        match ip_addr.clone() {
            Ok(ip) => {
                let address_str = format!("{}:8012", ip.to_string());
                match address_str.parse::<SocketAddr>() {
                    Ok(socket_address) => {
                        if let Ok(mut stream) = TcpStream::connect(socket_address) {
                            let response_wrapper = ResponseWrapper::Delete(res.clone());
                            let serialized_response = bincode::serialize(&response_wrapper);

                            match serialized_response {
                                Ok(ser_res) => {
                                    println!("SENT DELETE RESPONSE BACK TO CLIENT (REPLICA NODE)");
                                    stream.write(&ser_res);
                                },
                                Err(_) => {
                                    eprintln!("Error in sending serialized response in send_delete_response_to_client");
                                }
                            }
                        } else {
                            println!("Failed to TCP send message SEND DELETE RESPONSE TO CLIENT");
                        }
                    },
                    Err(e) => {
                        eprintln!("Issue with parsing address in send_delete_response_to_client");
                    }
                }
            },
            Err(e) => {
                eprintln!("Issue with getting ip address in send_delete_response_to_client");
            }
        }

        Ok(())
    }

    pub fn start_listening(&mut self) {
        let self_clone_first = self.clone();
        let self_clone_second = self.clone();
        let mut self_clone_third = self.clone();
        let self_clone_fourth = self.clone();

        let clients_thread_handle = thread::spawn(move || {
            self_clone_first.listen_to_client_requests();
        });

        let leader_thread_handle = thread::spawn(move || {
            self_clone_second.listen_to_node_failure_requests();
        });

        let replica_request_listener_handle = thread::spawn(move || {
            self_clone_third.listen_to_write_requests_from_other_replicas();
        });

        let replica_response_listener_handle = thread::spawn(move || {
            self_clone_fourth.listen_to_node_failure_responses();
        });

        if let Ok(mut client_join_handle) = self.client_connection_join_handle.lock() {
            *client_join_handle = Some(clients_thread_handle);
        }

        if let Ok(mut leader_join_handle) = self.leader_connection_join_handle.lock() {
            *leader_join_handle = Some(leader_thread_handle);
        }

        if let Ok(mut replica_req_handle) = self.replica_connection_request_join_handle.lock() {
            *replica_req_handle = Some(replica_request_listener_handle);
        }

        if let Ok(mut replica_res_handle) = self.replica_connection_response_join_handle.lock() {
            *replica_res_handle = Some(replica_response_listener_handle);
        }
    }

    pub fn close_threads(&mut self) {
        if let Ok(mut handle) = self.client_connection_join_handle.lock() {
            if let Some(hdl) = handle.take() {
                hdl.join().expect("Client connection thread in Replica panicked!");
            }
        }

        if let Ok(mut handle) = self.leader_connection_join_handle.lock() {
            if let Some(hdl) = handle.take() {
                hdl.join().expect("Leader connection thread in Replica panicked!");
            }
        }

        if let Ok(mut handle) = self.replica_connection_request_join_handle.lock() {
            if let Some(hdl) = handle.take() {
                hdl.join().expect("Replica connection One thread in Replica panicked!");
            }
        }

        if let Ok(mut handle) = self.replica_connection_response_join_handle.lock() {
            if let Some(hdl) = handle.take() {
                hdl.join().expect("Replica connection Two connection thread in Replica panicked!");
            }
        }
    }
}
