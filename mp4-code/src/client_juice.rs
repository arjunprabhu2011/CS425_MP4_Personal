use std::fs::{self, File};
use std::process::Command;
use std::io::{Result, Write, Read};
use std::sync::Arc;

use mp3_code::{handle_get, handle_delete, wait_for_get_ack};
use crate::client_mj::ClientMapleJuice;
use crate::utils_messages_types::{JuiceHandlerRequest, MessageType, JuiceCompleteAck};
use crate::utils_funcs::{send_serialized_message, get_ip_addr};
use crate::utils_consts::MP4_PORT;

impl ClientMapleJuice {
    pub fn handle_juice_leader_request(&mut self, juice_handler_request: JuiceHandlerRequest) -> Result<()> {
        let mut temp_output_file = File::create("output_file_juice.txt")?;
        for key in juice_handler_request.vec_of_keys.iter() {
            let sdfs_file_to_open = format!("{}_{}.txt", juice_handler_request.sdfs_intermediate_filename_prefix, key);
            let local_intermediary_file = format!("local_{}.txt", sdfs_file_to_open);

            handle_get(sdfs_file_to_open.clone(), local_intermediary_file.clone(), (*self.machine_domain_name).clone(), self.sdfs_leader_domain_name.clone());

            let ret_get_ack = wait_for_get_ack(&self.machine_domain_name);

            if let Ok(true) = ret_get_ack {
                println!("File {} has been downloaded.", local_intermediary_file);

                let output = Command::new("python3")
                    .arg(juice_handler_request.juice_exe.clone())
                    .arg(local_intermediary_file.clone())
                    .output()
                    .expect("Failed to execute command");

                temp_output_file.write_all(&output.stdout);

                if juice_handler_request.delete_input == 1 {
                    handle_delete(sdfs_file_to_open, (*self.machine_domain_name).clone(), self.sdfs_leader_domain_name.clone());
                }
    
                fs::remove_file(local_intermediary_file.clone())?;
    
                println!("Local File '{}' has been deleted.", local_intermediary_file);
            } else {
                eprintln!("Timeout: File {} was not downloaded in time.", local_intermediary_file);
            }
        }

        drop(temp_output_file);

        let mut file = File::open("output_file_juice.txt")?;
        let mut contents = Vec::new();
        file.read_to_end(&mut contents)?;

        let juice_ack = MessageType::JuiceCompleteAckType(JuiceCompleteAck {
            domain_name: (*self.machine_domain_name).clone(),
            exe_file: juice_handler_request.juice_exe.clone(),
            contents_to_output: contents,
            sdfs_dest_filename: juice_handler_request.sdfs_dest_filename,
            delete_input: juice_handler_request.delete_input,
            sdfs_intermediate_prefix: juice_handler_request.sdfs_intermediate_filename_prefix
        });
        
        if let Ok(leader_name) = self.mj_leader_domain_name.lock() {
            let ip_addr = get_ip_addr(&leader_name);

            match ip_addr {
                Ok(ip) => {
                    let address_str = format!("{}:{}", ip.to_string(), MP4_PORT);
    
                    send_serialized_message(Arc::new(address_str), juice_ack);
                },
                Err(e) => {
                    eprintln!("Get ip address error in CLIENT JUICE HANDLE JUICE LEADER REQUEST: {}", e);
                }
            }
        }

        Ok(())
    }
}