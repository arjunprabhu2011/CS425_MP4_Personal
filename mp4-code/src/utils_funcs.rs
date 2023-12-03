use std::io::{self, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use rand::seq::SliceRandom;
use std::fs::{File, self};
use std::io::{BufRead, BufReader, Result};

use crate::utils_messages_types::MessageType;
use crate::utils_consts::INTERMEDIATE_FILE_PATH;

/**
 * Send a message via tcp to another address
 */
pub fn send_tcp_message_maplejuice(address: &str, message: &[u8]) -> std::io::Result<()> {
    // Connect to the server
    if let Ok(mut stream) = TcpStream::connect(address) {
        stream.write_all(message)?;
        stream.flush()?;
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::Other,
            "Could not connect to remote address to send_tcp_message",
        ))
    }
}

/**
 * Get the ip address based on a domain
 */
pub fn get_ip_addr(domain: &str) -> core::result::Result<Arc<String>, String> {
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

pub fn send_serialized_message(ip_to_send_to: Arc<String>, message: MessageType) {
    let serialized_handler_request = bincode::serialize(&message);

    match serialized_handler_request {
        Ok(ser_req) => {
            send_tcp_message_maplejuice(&ip_to_send_to, &ser_req);
        },
        Err(_) => {
            eprintln!("Error in sending serialized request in handle_juice_request");
        }
    }
}

pub fn split_file(file_name: &str, num_splits: u32) -> Result<Vec<(u64, u64)>> {
    let file = File::open(file_name)?;
    let reader = BufReader::new(file);

    let total_lines = reader.lines().count();
    let lines_per_split = total_lines / num_splits as usize;
    let mut extra_lines = total_lines % num_splits as usize;

    let mut result = Vec::new();
    let mut current_start = 1;

    for _ in 0..num_splits {
        let mut current_end = current_start + lines_per_split - 1;

        if extra_lines > 0 {
            current_end += 1;
            extra_lines -= 1;
        }

        result.push((current_start as u64, current_end as u64));
        current_start = current_end + 1;
    }

    Ok(result)
}

pub fn wait_for_file_creation(file_path: &str, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if fs::metadata(file_path).is_ok() {
            // File exists
            return true;
        }

        // Sleep for a short duration
        thread::sleep(Duration::from_millis(100));
    }
    false
}