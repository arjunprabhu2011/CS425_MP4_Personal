use csv::{Reader, ReaderBuilder, StringRecord, Writer};
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::process;
use std::io::BufWriter;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 6 {
        eprintln!("Usage: join_csv <file1_path> <file1_join_field_name> <file2_path> <file2_join_field_name> <prefix>");
        process::exit(1);
    }

    let file1_path = &args[1];
    let file1_join_field_name = &args[2];
    let file2_path = &args[3];
    let file2_join_field_name = &args[4];
    let prefix = &args[5];

    let file1 = File::open(file1_path).expect("Failed to open file 1");
    let file2 = File::open(file2_path).expect("Failed to open file 2");

    let mut rdr1 = ReaderBuilder::new().has_headers(true).from_reader(file1);
    let mut rdr2 = ReaderBuilder::new().has_headers(true).from_reader(file2);

    let file1_join_field_index = find_field_index(&mut rdr1, file1_join_field_name).expect("Field not found in file 1");
    let file2_join_field_index = find_field_index(&mut rdr2, file2_join_field_name).expect("Field not found in file 2");

    let mut file1_records_map = HashMap::new();
    let mut joined_records = Vec::new();

    for result in rdr1.records() {
        let record = result.expect("Error reading record from file 1");
        if let Some(join_value) = record.get(file1_join_field_index) {
            file1_records_map.insert(join_value.to_string(), record);
        }
    }

    for result in rdr2.records() {
        let record = result.expect("Error reading record from file 2");
        if let Some(join_value) = record.get(file2_join_field_index) {
            if let Some(file1_record) = file1_records_map.get(join_value) {
                let joined_record = format!("{} {}", record_to_string(file1_record), record_to_string(&record));
                joined_records.push(joined_record);
            }
        }
    }

    let optimal_line_count = calculate_optimal_line_count(joined_records.len());

    // Write the joined records to multiple files with the specified prefix
    for (index, chunk) in joined_records.chunks(optimal_line_count).enumerate() {
        let file_name = format!("{}_{}.csv", prefix, index);
        let mut wtr = Writer::from_writer(BufWriter::new(File::create(&file_name).expect("Failed to create file")));
        
        for record in chunk {
            wtr.write_record(record.split_whitespace()).expect("Failed to write record");
        }

        wtr.flush().expect("Failed to flush writer");
    }
}

fn find_field_index(rdr: &mut Reader<File>, field_name: &str) -> Option<usize> {
    rdr.headers().ok()?.iter().position(|f| f == field_name)
}

fn record_to_string(record: &StringRecord) -> String {
    record.iter().collect::<Vec<&str>>().join(" ")
}

fn calculate_optimal_line_count(total_lines: usize) -> usize {
    let desired_file_count = 10;
    (total_lines + desired_file_count - 1) / desired_file_count
}
