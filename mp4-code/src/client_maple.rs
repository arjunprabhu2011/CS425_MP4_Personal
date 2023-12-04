use std::collections::HashMap;
use std::fs::File;
use std::process::Command;
use std::sync::Arc;

use crate::client_mj::ClientMapleJuice;
use crate::utils_funcs::{send_serialized_message, get_ip_addr};
use crate::utils_messages_types::{MapleHandlerRequest, MapleCompleteAck, MessageType};
use crate::utils_consts::MP4_PORT_LEADER;
use std::io::{BufRead, BufReader, Result, Write};

/**
 * Represents a Client executing a maple exe or receiving an ack from the leader
 */
impl ClientMapleJuice {
    /**
     * Handle a request to execute the maple exe file
     */
    pub fn handle_maple_leader_request(&mut self, maple_handler_request: MapleHandlerRequest) -> Result<()> {
        let mut temp_output_file = File::create("input_data_local.csv")?;

        temp_output_file.write_all(&maple_handler_request.input_data);

        drop(temp_output_file);

        let output = Command::new("python3")
            .arg(maple_handler_request.maple_exe.clone())
            .arg(maple_handler_request.sql_file)
            .arg("input_data_local.csv")
            .output()
            .expect("Failed to execute command");

        let mut temp_key_value_file = File::create("temp_key_value_file.txt")?;

        temp_key_value_file.write_all(&output.stdout);

        drop(temp_key_value_file);

        let file = File::open("temp_key_value_file.txt")?;
        let reader = BufReader::new(file);

        let mut key_value_hashmap: HashMap<String, Vec<Vec<u8>>> = HashMap::new();

        for line in reader.lines() {
            let line = line?;

            let parts: Vec<&str> = line.splitn(2, ',').collect();

            if parts.len() == 2 {
                let mut value_bytes = parts[1].as_bytes().to_vec();
                value_bytes.push(b'\n');
                key_value_hashmap.entry(parts[0].to_string())
                    .or_insert_with(Vec::new)
                    .push(value_bytes);
            }
        }

        let maple_ack = MessageType::MapleCompleteAckType(MapleCompleteAck {
            domain_name: (*self.machine_domain_name).clone(),
            exe_file: maple_handler_request.maple_exe.clone(),
            key_to_values_output: key_value_hashmap,
            sdfs_intermediate_prefix: maple_handler_request.intermediate_prefix
        });

        if let Ok(leader_name) = self.mj_leader_domain_name.lock() {
            let ip_addr = get_ip_addr(&leader_name);

            match ip_addr {
                Ok(ip) => {
                    let address_str = format!("{}:{}", ip.to_string(), MP4_PORT_LEADER);
                    println!("HANDLE MAPLE LEADER REQUEST ADDRESS: {}", address_str);

                    send_serialized_message(Arc::new(address_str), maple_ack);
                },
                Err(e) => {
                    eprintln!("Get ip address error in CLIENT JUICE HANDLE JUICE LEADER REQUEST: {}", e);
                }
            }
        }

        Ok(())
    }
}