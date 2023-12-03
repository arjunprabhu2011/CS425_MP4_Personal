import pandas as pd
import sys
import re
import math

def read_csv(file_path):
    return pd.read_csv(file_path)

def read_query(file_path):
    with open(file_path, 'r') as file:
        query = file.readline()
        return query

def parse_query(query):
    match = re.search(r'WHERE (\w+)\.(\w+) = (\w+)\.(\w+)', query)
    if match:
        table1_field, table2_field = match.group(2), match.group(4)
        return table1_field, table2_field
    else:
        raise ValueError("Query format is incorrect or join fields are not specified.")

def join_datasets(dataset1, dataset2, join_field_1, join_field_2):
    return pd.merge(dataset1, dataset2, left_on=join_field_1, right_on=join_field_2)

def split_into_chunks(dataset, chunk_size):
    chunks = {}
    for i in range(0, len(dataset), chunk_size):
        chunk_key = f"Chunk_{i // chunk_size + 1}"
        chunk = dataset.iloc[i:i + chunk_size].reset_index(drop=True)
        chunks[chunk_key] = chunk
    return chunks

def calculate_ideal_chunk_size(dataset, desired_chunks):
    total_rows = len(dataset)
    if total_rows == 0:
        return 1  # Avoid division by zero
    chunk_size = max(1, math.ceil(total_rows / desired_chunks))
    return chunk_size

def main(file_path_1, file_path_2, query_file_path):
    desired_chunks = 10
    dataset1 = read_csv(file_path_1)
    dataset2 = read_csv(file_path_2)
    query = read_query(query_file_path)

    join_field_1, join_field_2 = parse_query(query)
    combined_dataset = join_datasets(dataset1, dataset2, join_field_1, join_field_2)

    ideal_chunk_size = calculate_ideal_chunk_size(combined_dataset, desired_chunks)
    chunks = split_into_chunks(combined_dataset, ideal_chunk_size)

    for key, chunk in chunks.items():
        print(f"{key}:")
        print(chunk.to_csv(index=False))
        print()

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python joiner.py <file_path_1> <file_path_2> <query_file_path>")
        sys.exit(1)

    main(sys.argv[1], sys.argv[2], sys.argv[3])
