import pandas as pd
import re
import sys
import math 

def read_csv(file_path):
    return pd.read_csv(file_path)

def read_sql_query(file_path):
    with open(file_path, 'r') as file:
        return file.read()

def extract_regex_from_sql(sql_query):
    # Extract regex from a query like "SELECT ALL FROM Dataset WHERE "regex""
    match = re.search(r"WHERE\s+\"([^\"]+)\"", sql_query)
    if match:
        return match.group(1)
    return ""

def filter_dataset(dataset, regex):
    matching_rows = []

    for index, row in dataset.iterrows():
        # Convert the entire row to a single string
        row_str = ','.join(row.astype(str))
        
        # Search for the regex pattern in the row string
        if re.search(regex, row_str):
            # If there is a match, add the original row to the list
            matching_rows.append(row)

    # Convert the list of matching rows
    return pd.DataFrame(matching_rows)

def split_into_chunks(dataset, chunk_size):
    # Split dataset into chunks of `chunk_size` and return as a dictionary
    chunks = {}
    for i in range(0, len(dataset), chunk_size):
        chunk_key = f"Chunk_{i // chunk_size + 1}"
        chunk = dataset.iloc[i:i + chunk_size].reset_index(drop=True)
        chunks[chunk_key] = chunk
    return chunks

def calculate_ideal_chunk_size(dataset, desired_chunks):
    total_rows = len(dataset)
    if total_rows == 0:
        return 1 # Avoid division by zero
    chunk_size = max(1, math.ceil(total_rows / desired_chunks))
    return chunk_size


def main(csv_file, sql_query_file):
    desired_chunks = 10
    dataset = read_csv(csv_file)
    sql_query = read_sql_query(sql_query_file)
    regex = extract_regex_from_sql(sql_query)
    filtered_dataset = filter_dataset(dataset, regex)
    
    ideal_chunk_size = calculate_ideal_chunk_size(filtered_dataset, desired_chunks)
    chunks = split_into_chunks(filtered_dataset, ideal_chunk_size)

    # Print each chunk in CSV format
    for key, chunk in chunks.items():
        print(f"{key}:")
        print(chunk.to_csv(index=False))
        print()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <csv_file_path> <sql_query_file_path>")
        sys.exit(1)

    main(sys.argv[1], sys.argv[2])
