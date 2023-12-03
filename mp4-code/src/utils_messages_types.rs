use std::collections::HashMap;

use serde::{Serialize, Deserialize};

/**
 * This is used for serialization and deserialization purposes with bincode
 */
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum MessageType {
    MapleRequestType(MapleRequest),
    MapleHandlerRequestType(MapleHandlerRequest),
    MapleCompleteAckType(MapleCompleteAck),
    JuiceRequestType(JuiceRequest),
    JuiceHandlerRequestType(JuiceHandlerRequest),
    JuiceCompleteAckType(JuiceCompleteAck),
    FullTaskCompleteAckType(FullTaskCompleteAck)
}

/**
 * This represents the request that a regular client makes to the leader
 * i.e. when it types maple arg1 arg2 ..., it sends this request to the leader to start the task
 */
#[derive(Serialize, Deserialize, Debug, PartialEq, Hash, Eq, Clone)]
pub struct MapleRequest {
    pub original_client_domain_name: String,
    pub maple_exe: String,
    pub intermediate_prefix: String,
    pub source_file: String, //Input data into maple
    pub sql_file: String,
    pub num_maples: u16,
}  

/**
 * This represents the maple request that the leader sends to a single maple-juice client
 */
#[derive(Serialize, Deserialize, Debug, PartialEq, Hash, Eq, Clone)]
pub struct MapleHandlerRequest {
    pub maple_exe: String,
    pub intermediate_prefix: String,
    pub sql_file: String,
    pub input_data: Vec<u8>,
    pub start_line: u64 //Just used so that the worker can make a temporary file that is the input into the maple_exe
}  

/**
 * This represents an Ack that single maple-juice client sends back to the leader
 * when it has finished its maple task on its partition of the input data
 */
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct MapleCompleteAck {
    pub domain_name: String, //Domain name of MapleJuice Client that finished task
    pub exe_file: String,
    pub key_to_values_output: HashMap<String, Vec<Vec<u8>>>,
    pub sdfs_intermediate_prefix: String
}

/**
 * This is simply an enum that represents the Maple or Juice.
 * Mostly used in sending acknowledgement back to original client
 */
#[derive(Serialize, Deserialize, Debug, PartialEq, Hash, Eq, Clone)]
pub enum TaskType {
    Maple,
    Juice,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Hash, Eq, Clone)]
/**
 * This represents the leader's bookkeeping struct on the current task that is running. 
 * The leader updates servers_left_to_complete_task upon failures and when it gets back 
 * a MapleCompleteAck or JuiceCompleteAck
 */
pub struct MJTask {
    pub original_client_domain_name: String, //Need this to eventually send an ack back to client when leader gets all acks
    pub task_type: TaskType,
    pub servers_left_to_complete_task: Vec<String>,
    pub exe_file: String
} 

/**
 * This represents the request that a regular client makes to the leader
 * i.e. when it types juice arg1 arg2 ..., it sends this request to the leader to start the task
 */
#[derive(Serialize, Deserialize, Debug, PartialEq, Hash, Eq, Clone)]
pub struct JuiceRequest {
    pub original_client_domain_name: String,
    pub juice_exe: String,
    pub num_juices: u16,
    pub sdfs_intermediate_filename_prefix: String,
    pub sdfs_dest_filename: String,
    pub delete_input: u8, //0 = don't delete intermediate files, 1 = delete
    pub hash_or_range: u8 //0 = hash, 1 = range
} 

/**
 * This represents the juice request that the leader sends to a single maple-juice client
 */
#[derive(Serialize, Deserialize, Debug, PartialEq, Hash, Eq, Clone)]
pub struct JuiceHandlerRequest {
    pub juice_exe: String,
    pub sdfs_intermediate_filename_prefix: String,
    pub sdfs_dest_filename: String,
    pub delete_input: u8,
    pub vec_of_keys: Vec<String>
} 

/**
 * This represents an Ack that single maple-juice client sends back to the leader
 * when it has finished its juice task on its partition of the keys
 */
#[derive(Serialize, Deserialize, Debug, PartialEq, Hash, Eq, Clone)]
pub struct JuiceCompleteAck {
    pub domain_name: String, //Domain name of MapleJuice Client that finished task
    pub exe_file: String,
    pub contents_to_output: Vec<u8>,
    pub sdfs_dest_filename: String,
    pub sdfs_intermediate_prefix: String,
    pub delete_input: u8
}

/**
 * This represents the Ack that the leader sends back to the original client that typed the maple or juice command
 */
#[derive(Serialize, Deserialize, Debug, PartialEq, Hash, Eq, Clone)]
pub struct FullTaskCompleteAck {
    pub task_type: TaskType, //Task Type for the ACK
    pub exe_file: String, //Exec file that the task ran
}
