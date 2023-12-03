use std::io::{Write, self, Read};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::SystemTime;
use std::collections::HashMap;
use std::fs::File;
use std::io::Result;


use std::env;
use std::net::UdpSocket;
use std::process::exit;

pub mod mp2; 
pub mod leader; 
pub mod replica;
pub mod utils;
pub mod client;

pub use mp2::{
    PORT_8000,
    node_join,
    membership_listener,
    membership_sender,
    check_list_for_failure,
    listen_for_suspicion_and_drop_rate_changes
};

pub use utils::{
    get_ip_addr,
    DOMAINS
};

pub use mp3_code::run_cli;

pub use mp3_code::NodeStatus;

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
    let leader = Arc::new(Mutex::new(leader::LeaderListener::new(domain_name_arc, membership_list_arc_clone)));

    let leader_clone = Arc::clone(&leader);
    let leader_listener_thread = thread::spawn(move || {
        if let Ok(mut leader_node) = leader_clone.lock() {
            leader_node.start_listening();
            leader_node.close_threads();
        }
    });

    let domain_name_arc = Arc::new(domain_name.clone());
    let leader_name_arc = Arc::new(Mutex::new(DOMAINS[2].to_string()));
    let client = Arc::new(Mutex::new(client::Client::new(domain_name_arc, leader_name_arc)));

    let client_clone = Arc::clone(&client);
    let client_listener_thread = thread::spawn(move || {
        if let Ok(mut client_node) = client_clone.lock() {
            client_node.start_listening();
            client_node.close_threads();
        }
    });

    let domain_name_arc = Arc::new(domain_name.clone());
    let leader_name_arc = Arc::new(Mutex::new(DOMAINS[2].to_string()));
    let files = Arc::new(Mutex::new(Vec::new()));
    let files_cloned = Arc::clone(&files.clone());
    let replica = Arc::new(Mutex::new(replica::ReplicaListener::new(domain_name_arc, leader_name_arc, files)));

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
    let leader_name_arc = Arc::new(Mutex::new(DOMAINS[2].to_string()));

    let failure_detection_thread = thread::spawn(move || {
        check_list_for_failure(machine_num, &membership_list_arc_clone, &suspicion_bool_clone, &leader_clone, &leader_name_arc);
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
    let leader_name_arc = Arc::new(Mutex::new(DOMAINS[2].to_string()));
    let cli_thread = thread::spawn(move || {
        run_cli(&membership_list_arc_clone, machine_num, &suspicion_bool_clone, &drop_rate_clone, domain_name_arc, leader_name_arc, files_cloned);
    });

    // Join threads
    if let Err(e) = leader_listener_thread.join() {
        println!("Failed to join listener thread: {:?}", e);
    }

    if let Err(e) = client_listener_thread.join() {
        println!("Failed to join listener thread: {:?}", e);
    }

    if let Err(e) = replica_listener_thread.join() {
        println!("Failed to join listener thread: {:?}", e);
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