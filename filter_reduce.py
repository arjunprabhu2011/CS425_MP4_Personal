import pandas as pd
import sys

def read_chunks(file_paths):
    chunks = []
    for path in file_paths:
        chunk = pd.read_csv(path)
        chunks.append(chunk)
    return chunks

def concatenate_chunks(chunks):
    # Concatenate all chunks into a single DataFrame
    return pd.concat(chunks, ignore_index=True)

def main(chunk_file_paths):
    chunks = read_chunks(chunk_file_paths)
    combined_dataset = concatenate_chunks(chunks)
    
    # Output the combined dataset
    print(combined_dataset.to_csv(index=False))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python reducer.py <chunk_file_path_1> <chunk_file_path_2> ...")
        sys.exit(1)

    main(sys.argv[1:])
