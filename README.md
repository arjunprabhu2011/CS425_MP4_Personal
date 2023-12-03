# CS425_MP4_Personal
Team: Akul Gupta, Arjun Prabhu


## Build Instructions:

1. You must first have the Rust compiler (rustc) and Cargo installed. You can either run curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh to install these or visit https://www.rust-lang.org/tools/install for more installation options. Follow the instructions for the default installation of Rust when prompted.
2. If you are installing Rust for the first time, you will also need to run the following command to add cargo to your path for the current shell/session: source "$HOME/.cargo/env"
3. Run make in the project root directory. This will build an executable ./main.



## Running Instructions: 

1. To use the main system, build with `make`.
2. To clean up files, run `make clean`.  

### CLI Commands

This CLI supports a set of commands for managing the membership list, handling node status, and file operations. Below is the list of available commands:

- `list_mem`: Lists the membership of the system.
- `list_self`: Lists the current machine's details in the membership.
- `leave`: Initiates the current node's graceful leave from the membership.
- `enable suspicion`: Activates the suspicion mechanism for failure detection.
- `disable suspicion`: Deactivates the suspicion mechanism for failure detection.

### File Operations

- `put <localfilename> <sdfsfilename>`: Uploads a file from local storage to the distributed file system.
- `get <sdfsfilename> <localfilename>`: Downloads a file from the distributed file system to local storage.
- `delete <sdfsfilename>`: Deletes a file from the distributed file system.
- `ls <sdfsfilename>`: Lists all nodes where the file is currently being stored.
- `store`: Lists all files currently stored on the local node.

### Multi-Node Read Operations

- `multiread <sdfsfilename> <localfilename> <vm1> <vm2> ...`: Initiates a read operation from multiple nodes for the specified file.

### Notes

- Replace `<localfilename>` with the name of the file on your local machine.
- Replace `<sdfsfilename>` with the name of the file in the distributed file system.
- Replace `<vm1> <vm2> ...` with the actual VM identifiers you wish to read from.
- Ensure that the correct number of arguments is provided for each command.
- If an invalid command or an incorrect number of arguments is entered, the system will respond with "Invalid command or incorrect number of arguments."