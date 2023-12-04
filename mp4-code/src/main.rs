use std::io::{self};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::SystemTime;
use std::collections::HashMap;


use std::env;
use std::net::UdpSocket;
use std::process::exit;

mod client_mj;
mod client_maple;
mod client_juice;
mod leader_mj;
mod leader_maple;
mod leader_juice;
mod utils_consts;
mod utils_funcs;
mod utils_messages_types;
mod mp2_code_for_mp4;

pub use client_mj::*;
pub use client_maple::*;
pub use client_juice::*;
pub use leader_mj::*;
pub use leader_maple::*;
pub use leader_juice::*;
pub use utils_consts::*;
pub use utils_funcs::*;
pub use utils_messages_types::*;
pub use mp2_code_for_mp4::*;

use mp3_code::NodeStatus;
use mp3_code::leader::LeaderListener;
use mp3_code::client::Client;
use mp3_code::replica::ReplicaListener;
use mp3_code::handle_get;
use mp3_code::handle_put;
use mp3_code::handle_delete;
use mp3_code::handle_ls;
use mp3_code::handle_multiread;
use mp3_code::print_current_files;
use mp3_code::wait_for_put_ack;

// Need to add constants like file directory info, roots, etc...
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
    let domain_name_arc = Arc::new(domain_name.clone());
    let leader = Arc::new(Mutex::new(LeaderListener::new(domain_name_arc, membership_list_arc_clone)));

    let sdfs_leader_name_arc = Arc::new(Mutex::new(DOMAINS[2].to_string()));

    let membership_list_arc_clone = Arc::clone(&membership_list);
    let leader_mj = Arc::new(Mutex::new(LeaderMapleJuice::new(Arc::new(domain_name.clone()), sdfs_leader_name_arc.clone(), membership_list_arc_clone)));
    let leader_mj_name_arc = Arc::new(Mutex::new(DOMAINS[1].to_string()));

    let leader_mj_clone = Arc::clone(&leader_mj);
    let leader_mj_listener_thread = thread::spawn(move || {
        if let Ok(mut leader_mj_node) = leader_mj_clone.lock() {
            leader_mj_node.start_listening();
        }
    });

    let leader_mj_clone_name = Arc::clone(&leader_mj_name_arc);
    let sdfs_leader_clone_name = Arc::clone(&sdfs_leader_name_arc);
    let client_mj = Arc::new(Mutex::new(ClientMapleJuice::new(Arc::new(domain_name.clone()), leader_mj_clone_name, sdfs_leader_clone_name)));

    let client_mj_clone = Arc::clone(&client_mj);
    let client_mj_listener_thread = thread::spawn(move || {
        if let Ok(mut client_mj_node) = client_mj_clone.lock() {
            client_mj_node.start_listening();
        }
    });

    let leader_clone = Arc::clone(&leader);
    let leader_listener_thread = thread::spawn(move || {
        if let Ok(mut leader_node) = leader_clone.lock() {
            leader_node.start_listening();
            leader_node.close_threads();
        }
    });

    let domain_name_arc = Arc::new(domain_name.clone());
    let leader_name_arc = Arc::clone(&sdfs_leader_name_arc);
    let client = Arc::new(Mutex::new(Client::new(domain_name_arc, leader_name_arc)));

    let client_clone = Arc::clone(&client);
    let client_listener_thread = thread::spawn(move || {
        if let Ok(mut client_node) = client_clone.lock() {
            client_node.start_listening();
            client_node.close_threads();
        }
    });

    let domain_name_arc = Arc::new(domain_name.clone());
    let leader_name_arc = Arc::clone(&sdfs_leader_name_arc);
    let files = Arc::new(Mutex::new(Vec::new()));
    let files_cloned = Arc::clone(&files.clone());
    let replica = Arc::new(Mutex::new(ReplicaListener::new(domain_name_arc, leader_name_arc, files)));

    let replica_clone = Arc::clone(&replica);
    let replica_listener_thread = thread::spawn(move || {
        if let Ok(mut replica_node) = replica_clone.lock() {
            replica_node.start_listening();
            replica_node.close_threads();
        }
    });

    let membership_list_arc_clone = Arc::clone(&membership_list);
    let udp_socket_clone = Arc::clone(&udp_socket);
    let drop_rate_clone = Arc::clone(&drop_rate);
    let listening_thread = thread::spawn(move || {
        membership_listener(machine_num, &membership_list_arc_clone, &udp_socket_clone, &drop_rate_clone);
    });

    let membership_list_arc_clone = Arc::clone(&membership_list);
    let udp_socket_clone = Arc::clone(&udp_socket);
    let sending_thread = thread::spawn(move || {
        membership_sender(&membership_list_arc_clone, &udp_socket_clone);
    });

    let membership_list_arc_clone = Arc::clone(&membership_list);
    let suspicion_bool_clone = Arc::clone(&suspicion_active);
    let leader_clone = Arc::clone(&leader);
    let leader_name_arc = Arc::clone(&sdfs_leader_name_arc);
    let leader_mj_clone = Arc::clone(&leader_mj);
    let leader_mj_name_arc = Arc::clone(&leader_mj_name_arc);
    let leader_mj_clone_name = Arc::clone(&leader_mj_name_arc);

    let failure_detection_thread = thread::spawn(move || {
        check_list_for_failure(machine_num, &membership_list_arc_clone, &suspicion_bool_clone, &leader_clone, &leader_name_arc, &leader_mj_clone, &leader_mj_clone_name);
    });

    let suspicion_bool_clone = Arc::clone(&suspicion_active);
    let drop_rate_clone = Arc::clone(&drop_rate);
    let suspicion_listener_thread = thread::spawn(move || {
        if let Err(e) = listen_for_suspicion_and_drop_rate_changes(&suspicion_bool_clone, machine_num, &drop_rate_clone) {
            eprintln!("Error reading from suspicion stream: {}", e);
        }
    });

    let membership_list_arc_clone = Arc::clone(&membership_list);
    let suspicion_bool_clone = Arc::clone(&suspicion_active);
    let drop_rate_clone = Arc::clone(&drop_rate);
    let domain_name_arc = Arc::new(domain_name.clone());
    let leader_name_arc = Arc::clone(&sdfs_leader_name_arc);
    let leader_mj_clone_name = Arc::clone(&leader_mj_name_arc); 
    let cli_thread = thread::spawn(move || {
        run_cli_mp4(&membership_list_arc_clone, machine_num, &suspicion_bool_clone, &drop_rate_clone, domain_name_arc, leader_name_arc, leader_mj_clone_name, files_cloned);
    });

    // Join threads
    if let Err(e) = leader_mj_listener_thread.join() {
        println!("Failed to join MapleJuice Leader listener thread: {:?}", e);
    }

    if let Err(e) = client_mj_listener_thread.join() {
        println!("Failed to join MapleJuice Client listener thread: {:?}", e);
    }

    if let Err(e) = leader_listener_thread.join() {
        println!("Failed to join listener SDFS Leader thread: {:?}", e);
    }

    if let Err(e) = client_listener_thread.join() {
        println!("Failed to join listener SDFS Client thread: {:?}", e);
    }

    if let Err(e) = replica_listener_thread.join() {
        println!("Failed to join Replica listener thread: {:?}", e);
    }

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
 * arc_drop_rate - the drop rate 
 */
pub fn run_cli_mp4(membership_list: &Arc<Mutex<HashMap<(String, usize, SystemTime), (u64, u64, NodeStatus)>>>, machine_number: usize, suspicion_active: &Arc<Mutex<bool>>, arc_drop_rate: &Arc<Mutex<u32>>, machine_domain_name: Arc<String>, leader_domain_name: Arc<Mutex<String>>, leader_mj_domain_name: Arc<Mutex<String>>, files: Arc<Mutex<Vec<(String, u16)>>>) {
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
            } else if input.starts_with("get") {
                let sdfs_file = input.split(' ').nth(1).unwrap_or("");
                let local_file = input.split(' ').nth(2).unwrap_or("");
                
                handle_get(sdfs_file.to_string(), local_file.to_string(), machine_domain_name.to_string(), leader_domain_name.clone());
            } else if input.starts_with("put") {
                let sdfs_file = input.split(' ').nth(2).unwrap_or("");
                let local_file = input.split(' ').nth(1).unwrap_or("");
                println!("ENTERRED PUT");
                handle_put(sdfs_file.to_string(), local_file.to_string(), machine_domain_name.to_string(), leader_domain_name.clone())
            } else if input.starts_with("delete") {
                let sdfs_file = input.split(' ').nth(1).unwrap_or("");

                handle_delete(sdfs_file.to_string(), machine_domain_name.to_string(), leader_domain_name.clone());
            } else if input.starts_with("ls") {
                let sdfs_file = input.split(' ').nth(1).unwrap_or("");

                handle_ls(sdfs_file.to_string(), machine_domain_name.to_string(), leader_domain_name.clone());
            } else if input.starts_with("store") {
                print_current_files(files.clone());
            } else if input.starts_with("multiread") {
                let args: Vec<&str> = input.split_whitespace().collect();

                handle_multiread(&args, leader_domain_name.clone());
            } else if input.starts_with("maple") {
                let maple_executable = input.split(' ').nth(1).unwrap_or("");
                let sql_data = input.split(' ').nth(2).unwrap_or("");
                let number_of_maples = input.split(' ').nth(3).unwrap_or("");
                let prefix = input.split(' ').nth(4).unwrap_or("");
                let source_data = input.split(' ').nth(5).unwrap_or("");

                let num_maples_u16 = number_of_maples.parse::<u16>();

                if let Ok(num_maples) = num_maples_u16 {
                    send_maple_request(machine_domain_name.clone(), prefix.to_string(), maple_executable.to_string(), source_data.to_string(), sql_data.to_string(), num_maples, leader_mj_domain_name.clone(), leader_domain_name.clone());
                }
            } else if input.starts_with("juice") {
                let juice_executable = input.split(' ').nth(1).unwrap_or("");
                let number_of_juices = input.split(' ').nth(2).unwrap_or("");
                let prefix = input.split(' ').nth(3).unwrap_or("");
                let dest_file = input.split(' ').nth(4).unwrap_or("");
                let delete_input_flag = input.split(' ').nth(5).unwrap_or("");
                let hash_range_flag = input.split(' ').nth(6).unwrap_or("");

                let num_juices_u16 = number_of_juices.parse::<u16>();
                let delete_input_u8 = delete_input_flag.parse::<u8>();
                let hash_range_u8 = hash_range_flag.parse::<u8>();

                if let Ok(num_juices) = num_juices_u16 {
                    if let Ok(delete_input) = delete_input_u8 {
                        if let Ok(hash_range) = hash_range_u8 {
                            send_juice_request(machine_domain_name.clone(), prefix.to_string(), juice_executable.to_string(), dest_file.to_string(), num_juices, delete_input, hash_range, leader_mj_domain_name.clone())
                        }
                    }
                }
            }
        }
    }   
}

/**
 * Send Maple Request to Leader MJ
 */
pub fn send_maple_request(machine_domain_name: Arc<String>, sdfs_intermediate_prefix: String, maple_executable: String, source_data_path: String, sql_path: String, number_maples: u16, leader_domain_name: Arc<Mutex<String>>, sdfs_leader_domain_name: Arc<Mutex<String>>) {
    let maple_request = MessageType::MapleRequestType(MapleRequest {
        original_client_domain_name: (*machine_domain_name).clone(),
        intermediate_prefix: sdfs_intermediate_prefix,
        maple_exe: maple_executable,
        source_file: format!("sdfs_{}",source_data_path.clone()),
        sql_file: sql_path,
        num_maples: number_maples
    });

    handle_put(format!("sdfs_{}",source_data_path.clone()), source_data_path, (*machine_domain_name).clone(), sdfs_leader_domain_name);

    let ret_put_ack = wait_for_put_ack(&machine_domain_name);

    if let Ok(true) = ret_put_ack {
        if let Ok(leader) = leader_domain_name.lock() {
            let ip_addr = get_ip_addr(&leader);
    
            match ip_addr {
                Ok(ip) => {
                    let address_str = format!("{}:{}", ip.to_string(), MP4_PORT_LEADER);

                    println!("SENT MAPLE REQUEST TO LEADER ADDRESS: {}", address_str);

                    println!("SENT MAPLE REQUEST!");
    
                    send_serialized_message(Arc::new(address_str), maple_request);
                },
                Err(e) => {
                    eprintln!("Get ip address error in SEND MAPLE REQUEST: {}", e);
                }
            }
        }
    }
}

/**
 * Send Juice Request to leader mj
 */
pub fn send_juice_request(machine_domain_name: Arc<String>, sdfs_intermediate_prefix: String, juice_executable: String, sdfs_dest_file: String, number_juices: u16, delete_input_bool: u8, hash_range: u8, leader_maplejuice_domain_name: Arc<Mutex<String>>) {
    let juice_request = MessageType::JuiceRequestType(JuiceRequest {
        original_client_domain_name: (*machine_domain_name).clone(),
        juice_exe: juice_executable,
        num_juices: number_juices,
        sdfs_intermediate_filename_prefix: sdfs_intermediate_prefix,
        sdfs_dest_filename: sdfs_dest_file,
        delete_input: delete_input_bool,
        hash_or_range: hash_range
    });

    if let Ok(leader) = leader_maplejuice_domain_name.lock() {
        let ip_addr = get_ip_addr(&leader);

        match ip_addr {
            Ok(ip) => {
                let address_str = format!("{}:{}", ip.to_string(), MP4_PORT_LEADER);
                println!("SENT JUICE REQUEST TO LEADER ADDRESS: {}", address_str);

                println!("SENT JUICE REQUEST!");

                send_serialized_message(Arc::new(address_str), juice_request);
            },
            Err(e) => {
                eprintln!("Get ip address error in CLIENT JUICE HANDLE JUICE LEADER REQUEST: {}", e);
            }
        }
    }
}

