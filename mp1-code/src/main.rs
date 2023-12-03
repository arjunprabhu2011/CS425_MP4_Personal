use std::env;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::process::{Command, exit};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

extern crate lru;

use lru::LruCache;
use std::num::NonZeroUsize;

//List of all machines in the distributed system
const IP_ADDRESSES: [&str; 10] = [
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

/**
 * Helper method to start server and listen to socket connections
 * and data sent by each client through the socket address
 * @addr        The address the server is running on
 * @clients     A vector of all the clients
 * @start_time  The start time to pass into handle client to calculate latency for the entire grep
 * @total_lines The total lines grepped for all the clients combined
 */
fn run_server(addr: SocketAddr, clients: Arc<Mutex<Vec<TcpStream>>>, start_time: Arc<Mutex<Instant>>, total_lines: Arc<Mutex<i32>>) {
    if let Ok(listener) = TcpListener::bind(addr) {
        println!("Server running on {}", addr);
        //Infinite loop to keep listening to the socket for connections
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    if let Ok(mut locked_clients) = clients.lock() {
                        locked_clients.push(stream.try_clone().expect("Failed to clone stream"));
                    }

                    let clients_clone = Arc::clone(&clients);
                    let cli_start_time = Arc::clone(&start_time);
                    let total_lines_cloned = Arc::clone(&total_lines);
                    //Spawn a separate thread to handle each possible client
                    thread::spawn(move || {
                        handle_client(stream, clients_clone, cli_start_time, total_lines_cloned);
                    });
                },
                Err(e) => {
                    eprintln!("Connection failed: {}", e);
                }
            }
        }
    } else {
        eprintln!("Could not bind to address {}", addr);
    }
}

/**
 * Helper method to detect 'grep {flags} {pattern}' in the CLI (the terminal)
 * @clients     A vector of all the clients
 * @start_time  The start time to pass into handle client to calculate latency for the entire grep
 * @total_lines The total lines grepped for all the clients combined
 */
fn run_cli(clients: Arc<Mutex<Vec<TcpStream>>>, start_time: Arc<Mutex<Instant>>, total_lines: Arc<Mutex<i32>>) {
    loop {
        let mut input = String::new();
        //Read from standard in and detect if "grep {pattern}" is typed
        if io::stdin().read_line(&mut input).is_ok() {
            let input = input.trim();

            if input.starts_with("grep") {
                let mut grep_args: Vec<&str> = input.split_whitespace().collect();
                grep_args.remove(0);

                let mut command = Command::new("grep");
                command.arg("-r");

                //Read the args from the grep command
                for arg in grep_args {
                    command.arg(arg);
                }
                
                command.arg("logs/");

                let mut line_count = 0;

                //Perform the grep on this machine first before sending to other machines
                match command.output() {
                    Ok(output) => {
                        let grep_output = String::from_utf8_lossy(&output.stdout);

                        //If the grep command contains -c, handle the line count differently
                        if input.contains(&"-c") {
                            let grep_output_lines = grep_output.lines();

                            for line in grep_output_lines {
                                let count_for_file = line.split(':').nth(1).unwrap_or("");
                                line_count += count_for_file.parse().unwrap_or(0);
                            }

                            let full_message = format!("{}Line Count for Current Machine: {}\n", grep_output, line_count); 

                            print!("{}", full_message);
                        } else {
                            let line_count = grep_output.lines().count();
    
                            let full_message = format!("{}\nLine Count for current machine: {}\n", grep_output, line_count);

                            print!("{}", full_message);
                        }
                    },
                    Err(_) => {
                        eprintln!("Error in Grepping on Querying Machine");
                    }
                }

                //Update the total lines to be just the lines from this machine
                //This needs to be done to overwrite total_lines from the previous grep call
                match total_lines.lock() {
                    Ok(mut tot_lines) => {
                        *tot_lines = line_count;
                    }, 
                    Err(_e) => {
                        eprintln!("Error in finding line with Count");
                    }
                }

                //Write the grep command to the stream and start the timer for latency calculations
                if let Ok(locked_clients) = clients.lock() {
                    for client in locked_clients.iter() {
                        let mut client = client.try_clone().expect("Failed to clone client");
                        let _ = client.write(input.as_bytes());

                        match start_time.lock() {
                            Ok(mut time_start) => {
                                *time_start = Instant::now();
                            }, 
                            Err(_e) => {
                                eprintln!("Error in setting time");
                            }

                        }
                        let _ = client.flush();
                    }
                }
            }
        }
    }
}

/**
 * Helper method to run the machine as a client. Spawns 
 * a new thread for each server that could send it a grep call
 * @addr    The address that the client is connecting to
 */
fn run_client(addr: SocketAddr) {
    thread::sleep(Duration::from_secs(30));
    //Store a cache in case the grep commands are repeated
    let mut grep_cache : LruCache<Vec<String>, String> = LruCache::new(NonZeroUsize::new(100).unwrap());

    if let Ok(mut stream) = TcpStream::connect(addr) {
        println!("Client joined to {:?}", addr);
        loop {
            let mut buffer = [0; 512];
            if let Ok(_read_bytes) = stream.read(&mut buffer) {
                if _read_bytes == 0 {
                    continue;
                }

                let received_data = String::from_utf8_lossy(&buffer[0.._read_bytes]);
                println!("Client received: {}", received_data.trim_end_matches(char::from(0)));

                if received_data.starts_with("grep") {
                    let mut grep_args: Vec<&str> = received_data.split_whitespace().collect();
                    grep_args.remove(0);
    
                    if grep_args.len() < 1 {
                        println!("Not enough arguments for grep.");
                        return;
                    }

                    println!("{:?}", grep_args);

                    let cache_check_args = grep_args.clone();
                    let mut sorted_grep_args: Vec<String> = cache_check_args.iter().map(|s| s.to_string()).collect();
                    sorted_grep_args.sort();

                    //Cache stores the sorted vector for the grep args and the pattern
                    if let Some(cached_output) = grep_cache.get(&sorted_grep_args) {
                        if let Ok(_) = stream.write(&cached_output.as_bytes()) {
                            let _ = stream.flush();
                        }
                    } else {
                        let mut command = Command::new("grep");
                        command.arg("-r");

                        let grep_args_clone = grep_args.clone();

                        for arg in grep_args_clone {
                            command.arg(arg);
                        }
                        
                        command.arg("logs/");

                        match command.output() {
                            Ok(output) => {
                                let grep_output = String::from_utf8_lossy(&output.stdout);

                                if grep_args.contains(&"-c") {
                                    let grep_output_lines = grep_output.lines();
                                    let mut line_count = 0;

                                    let mut machine_number = 0;

                                    //Calculate the line count using string splitting
                                    for line in grep_output_lines {
                                        let count_for_file = line.split(':').nth(1).unwrap_or("");
                                        line_count += count_for_file.parse().unwrap_or(0);

                                        if line.contains("logs/machine") {
                                            let machine_number_as_string = line.split('.').nth(1).unwrap_or("");
                                            machine_number = machine_number_as_string.parse().unwrap_or(0);
                                        }
                                    }

                                    let full_message = format!("{}Line Count for Machine {}: {}\n", grep_output, machine_number, line_count);

                                    // Send the grep command output back to the server
                                    if let Ok(_) = stream.write(&full_message.as_bytes()) {
                                        let _ = stream.flush();
                                    }
                                    
                                    //Insert the grep command into the cache
                                    let grep_args_clone = grep_args.clone();
                                    let mut sorted_grep_args : Vec<String> = grep_args_clone.iter().map(|s| s.to_string()).collect();
                                    sorted_grep_args.sort();
                                    grep_cache.put(sorted_grep_args, full_message);
                                } else {
                                    let grep_output_lines = grep_output.lines();

                                    let mut machine_number = 0;

                                    //Calculate the line count using string splitting
                                    for line in grep_output_lines {
                                        if line.contains("logs/machine") {
                                            let machine_number_as_string = line.split('.').nth(1).unwrap_or("");
                                            machine_number = machine_number_as_string.parse().unwrap_or(0);
                                        }
                                    }

                                    let line_count = grep_output.lines().count();
    
                                    let full_message = format!("{}\nLine Count for Machine {}: {}\n", grep_output, machine_number, line_count);

                                    // Send the grep command output back to the server
                                    if let Ok(_) = stream.write(&full_message.as_bytes()) {
                                        let _ = stream.flush();
                                    }

                                    let grep_args_clone = grep_args.clone();
                                    let mut sorted_grep_args : Vec<String> = grep_args_clone.iter().map(|s| s.to_string()).collect();
                                    sorted_grep_args.sort();
                                    grep_cache.put(sorted_grep_args, full_message);
                                }
                            },
                            Err(e) => {
                                let error_message = format!("grep command failed - {}", e);
                                // Send the failure message back to the server
                                if let Ok(_) = stream.write(error_message.as_bytes()) {
                                    let _ = stream.flush();
                                }
                            }
                        }
                    }
                }
            } else {
                eprintln!("Failed to read from the server. Exiting client loop.");
                break;
            }
        }
    } else {
        eprintln!("Client is not connected to {}", addr);
    }
}

/**
 * Function to handle output sent back by a client. Helper function for run_server
 * @stream      The stream to read data from
 * @_clients    The vector of all clients
 * @start_time  The start time to pass into handle client to calculate latency for the entire grep
 * @total_lines The total lines grepped for all the clients combined
 */
fn handle_client(mut stream: TcpStream, _clients: Arc<Mutex<Vec<TcpStream>>>, start_time: Arc<Mutex<Instant>>, total_lines: Arc<Mutex<i32>>) {
    loop {
        let mut buffer = [0; 512];
        if let Ok(_read_bytes) = stream.read(&mut buffer) {
            if _read_bytes == 0 {
                continue
            }

            //Do this to remove any null bytes at the end of buffer at the end of the message
            let data = &buffer[0.._read_bytes];

            let string_of_data = std::str::from_utf8(data);

            // Write to stdout and calculate the latency of the operation
            match string_of_data {
                Ok(string_from_bytes) => {
                    print!("\n{}", string_from_bytes);
                    
                    if string_from_bytes.contains("Line Count for Machine") {
                        let end_time = Instant::now();
                        let stime_result = start_time.lock();
                        match stime_result {
                            Ok(stime) => {
                                let duration = end_time.duration_since(*stime).as_millis();
                                println!("Latency of grep: {}", duration);
                            },
                            Err(_e) => {
                                eprintln!("Error in finding start time");
                            }
                        }

                        let line = string_from_bytes.lines().filter(|&line| line.contains("Line Count for Machine")).next();
                        
                        //Calculate the total lines up until each client. The final 
                        //"Total Lines up until this client" message printed is the total
                        //lines for all clients
                        match line {
                            Some(l) => {
                                let count = l.split(':').nth(1).unwrap_or("");
                                
                                match total_lines.lock() {
                                    Ok(mut tot_lines) => {
                                        *tot_lines += count.replace(" ", "").parse().unwrap_or(0);
                                        println!("Total Lines up until this Client: {}", tot_lines);
                                    }, 
                                    Err(_e) => {
                                        eprintln!("Error in locking total line");
                                    }
                                }
                            },
                            None => eprintln!("Error in finding line with Count"),
                        }
                    }
                },
                Err(_e) => {
                    println!("Data not valid bytes");
                }
            }
        } else {
            eprintln!("Failed to read from client. Exiting client handler.");
            break;
        }
    }
}

/**
 * Main method to declare variables and create threads for each helper function
 */
fn main() {
    println!("Starting main");

    let args: Vec<String> = env::args().collect();
    let mut current_ip_address = Arc::new(String::new());

    for arg in args[1..].iter() {
        if arg.starts_with("--current-machine=") {
            let value_str = arg.split('=').nth(1).unwrap_or("");
            let current_machine_value: Option<usize> = value_str.parse().ok();

            //Match the current_machine_value from 1-10 to calculate the ip_address
            match current_machine_value {
                Some(index) => {
                    if index <= IP_ADDRESSES.len() && index > 0 {
                        let ip_address = &IP_ADDRESSES[index-1];
                        match get_ip_addr(ip_address) {
                            Ok(ip) => {
                                current_ip_address = ip;
                            },
                            Err(_) => {
                                println!("Could not resolve IP address.");
                                exit(1);
                            }
                        }
                    } else {
                        println!("Index out of bounds");
                        exit(1);
                    }
                },
                None => {
                    println!("current_machine_value is None");
                    exit(1);
                }
            }
        }
    }

    let addr: SocketAddr = format!("{}:7878", *current_ip_address).parse().unwrap_or_else(|_| {
        eprintln!("Failed to parse SocketAddr, {}", *current_ip_address);
        std::process::exit(1);
    });

    let clients = Arc::new(Mutex::new(Vec::new()));
    let start_time_for_grep = Arc::new(Mutex::new(Instant::now()));

    let total_grep_lines = Arc::new(Mutex::new(0));
    
    //Need to clone it to avoid borrowing/lifetime issues in Rust
    let total_grep_lines_cloned = Arc::clone(&total_grep_lines);

    let server_clients = Arc::clone(&clients);

    let server_start_time = Arc::clone(&start_time_for_grep);

    //Start a thread to run the server
    let server_thread = thread::spawn(move || {
        run_server(addr, server_clients, server_start_time, total_grep_lines);
    });

    let cli_clients = Arc::clone(&clients);

    let cli_start_time = Arc::clone(&start_time_for_grep);

    //Start a thread to detect changes to 
    //the CLI (detects "grep {flags} {pattern} typed in")
    let cli_thread = thread::spawn(move || {
        run_cli(cli_clients, cli_start_time, total_grep_lines_cloned);
    });

    let mut client_threads = Vec::new();


    //Start a thread to run this machine as a client for all
    //the other machines as servers
    for &ip_address in IP_ADDRESSES.iter() {
        match get_ip_addr(ip_address) {
            Ok(ip) => {
                if ip.as_str() != *current_ip_address {
                    let client_ip = ip.split(':').next().unwrap_or("");
                    let client_addr: SocketAddr = format!("{}:7878", client_ip).parse().unwrap_or_else(|_| {
                        eprintln!("Failed to parse client SocketAddr");
                        std::process::exit(1);
                    });
                    let client_thread = thread::spawn(move || {
                        run_client(client_addr);
                    });
                    client_threads.push(client_thread);
                }
            },
            Err(_) => {
                println!("Could not resolve IP address.");
                exit(1);
            }
        }
    }

    //Join the threads to handle fault tolerance
    server_thread.join().unwrap_or_else(|_| eprintln!("Server thread panicked"));
    cli_thread.join().unwrap_or_else(|_| eprintln!("CLI thread panicked"));

    for client_thread in client_threads {
        client_thread.join().unwrap_or_else(|_| eprintln!("Client thread panicked"));
    }
}

/**
 * Get the ip address based on a domain
 */
fn get_ip_addr(domain: &str) -> Result<Arc<String>, String> {
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