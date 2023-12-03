use std::{sync::Arc, net::ToSocketAddrs, time::{SystemTime, UNIX_EPOCH}};

use serde::{Serialize, Deserialize};
use std::collections::HashMap;

//Leader to replica connection will listen on port 7878
pub const DOMAINS: [&str; 10] = [
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

pub const NUM_SHARDS: u32 = 1;

/**
 * An Ack could either be a read or a write
 */
#[derive(Serialize, Deserialize, Debug, PartialEq, Hash, Eq, Clone)]
pub enum RequestType {
    Read,
    Write,
    Delete,
    NodeFailure,
}

/**
 * An Ack could either be a read or a write
 */
#[derive(Serialize, Deserialize, Debug, PartialEq, Hash, Eq, Clone)]
pub enum ClientRequest {
    Read(ReadRequest),
    Write(WriteRequest),
    Delete(DeleteRequest),
}

/**
 * The format of a read request made from a client to a replica machine
 */
#[derive(Serialize, Deserialize, Debug, PartialEq, Hash, Eq, Clone)]
pub struct ReadRequest {
    pub sdfs_filename: String,
    pub unix_filename: String,
    pub requesting_machine_domain_name: String,
    pub shard_number: u16,
    pub request_type: RequestType
}

/**
 * The format of a write request made from a client to a replica machine
 */
#[derive(Serialize, Deserialize, Debug, PartialEq, Hash, Eq, Clone)]
pub struct WriteRequest {
    pub sdfs_filename: String,
    pub unix_filename: String,
    pub data: Vec<u8>,
    pub requesting_machine_domain_name: String,
    pub shard_number: u16,
    pub request_type: RequestType
}

/**
 * The format of a delete request made from a client to a replica machine
 */
#[derive(Serialize, Deserialize, Debug, PartialEq, Hash, Eq, Clone)]
pub struct DeleteRequest {
    pub filename: String,
    pub requesting_machine_domain_name: String,
    pub shard_number: u16,
    pub request_type: RequestType
}

/**
 * The format of an ls request made from a client to a replica machine
 */
#[derive(Serialize, Deserialize, Debug, PartialEq, Hash, Eq, Clone)]
pub struct LsRequest {
    pub filename: String,
    pub requesting_machine_domain_name: String
}



/**
 * When a node fails, the leader must send this to the other replicas containing that node's shards
 */
#[derive(Serialize, Deserialize, Debug, PartialEq, Hash, Eq, Clone)]
pub struct NodeFailureRequest {
    pub filename: String,
    pub shard_number: u16,
    pub machine_domain_name_to_send_shard_to: String,
    pub request_type: RequestType
}

/**
 * An Ack could either be a read or a write
 */
#[derive(Serialize, Deserialize, Debug, PartialEq, Hash, Eq, Clone)]
pub enum ResponseType {
    Read,
    Write,
    Delete,
    NodeFailure,
    ReplicaList
}

/**
 * An Ack could either be a read or a write
 */
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum ResponseWrapper {
    Read(ReadResponse),
    Write(WriteResponse),
    Delete(DeleteResponse),
}

/**
 * This is the struct that the leader sends to the client after it makes a request
 */
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct ReplicaListResponse {
    pub sdfs_filename: String,
    pub unix_filename: String,
    pub file_shards: HashMap<u16, Vec<String>>, 
    pub response_type: ResponseType,
    pub request_type_to_send: RequestType
}

/**
 * This is the struct that the leader sends to the client after it makes a request
 */
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct LsResponse {
    pub filename: String,
    pub file_shards: HashMap<u16, Vec<String>>, 
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct ReadResponse {
    pub filename: String,
    pub unix_filename: String,
    pub data: Vec<u8>,
    pub shard_number: u16,
    pub responding_machine_domain_name: String,
    pub response_type: ResponseType
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct WriteResponse {
    pub filename: String,
    pub shard_number: u16,
    pub responding_machine_domain_name: String,
    pub response_type: ResponseType
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct DeleteResponse {
    pub filename: String,
    pub shard_number: u16,
    pub responding_machine_domain_name: String,
    pub response_type: ResponseType
}

/**
 * When a node fails and a new machine gets a shard, it must send this to the replica that sent it 
 */
#[derive(Serialize, Deserialize, Debug, PartialEq, Hash, Eq, Clone)]
pub struct NodeFailureResponse {
    pub filename: String,
    pub shard_number: u16,
    pub new_machine_holding_shard: String,
    pub response_type: ResponseType
}

/**
 * An Ack could either be a read or a write
 */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum AckType {
    Read,
    Write,
    Delete,
    NodeFailure
}

/**
 * This is the struct that represents any type of Ack sent by any machine
 */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RequestAck {
    pub ack_type: AckType,
    pub filename: String,
    pub machine_domain_name: String,
    pub new_machine_containing_shard: String,
    pub shard_num: u16,
}

/**
 * Get the ip address based on a domain
 */
pub fn get_ip_addr(domain: &str) -> Result<Arc<String>, String> {
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

/**
 * Get the current timestamp in nanoseconds
 */
pub fn get_current_timestamp() -> u128 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos(),
        Err(_) => 0, 
    }
}