use std::env;
use std::fs::File;
use std::process;
use std::io::{Write, BufWriter};
use regex::Regex;
use csv::{Reader, StringRecord, Writer};

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        eprintln!("Usage: filter_csv <file_path> <regex_pattern> <prefix>");
        process::exit(1);
    }

    let file_path = &args[1];
    let regex_pattern = &args[2];
    let prefix = &args[3];

    let re = Regex::new(regex_pattern).expect("Invalid regex pattern");

    // Count total lines in the file
    let file = File::open(file_path).expect("Failed to open file");
    let mut rdr = Reader::from_reader(file);
    let total_line_count = rdr.records().count();


    let optimal_line_count = calculate_optimal_line_count(total_line_count);

    // Process file and split into smaller files
    let file = File::open(file_path).expect("Failed to open file again");
    rdr = Reader::from_reader(file);
    let mut file_index = 0;
    let mut current_line_count = 0;
    let mut wtr = Writer::from_writer(BufWriter::new(File::create(format!("{}_{}.csv", prefix, file_index)).expect("Failed to create file")));

    for result in rdr.records() {
        let record = result.expect("Error reading record");
        if re.is_match(&record_to_string(&record)) {
            wtr.write_record(&record).expect("Failed to write record");
            current_line_count += 1;
        }

        if current_line_count >= optimal_line_count {
            wtr.flush().expect("Failed to flush writer");
            file_index += 1;
            current_line_count = 0;
            wtr = Writer::from_writer(BufWriter::new(File::create(format!("{}_{}.csv", prefix, file_index)).expect("Failed to create new file")));
        }
    }

    // Flush the last writer
    wtr.flush().expect("Failed to flush final writer");
}

fn calculate_optimal_line_count(total_lines: usize) -> usize {
    let desired_file_count = 10;
    (total_lines + desired_file_count - 1) / desired_file_count
}

fn record_to_string(record: &StringRecord) -> String {
    record.iter().collect::<Vec<&str>>().join(",")
}