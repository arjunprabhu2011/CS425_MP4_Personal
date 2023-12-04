import sys
import re
import csv

def extract_dataset_attribute_from_sql(sql_path, input_file_path):
    first_line = ""

    with open(sql_path, 'r') as file:
        first_line = file.readline().strip()
    
    datasets = re.findall(r"FROM (\w+), (\w+)", first_line)

    # Extracting the dataset names
    dataset_one, dataset_two = datasets[0]
    
    if dataset_one in input_file_path:
        attribute = ''
        with open(sql_path, 'r') as file:
            next(file)
            attribute = file.readline().strip()

        return attribute
    else:
        attribute = ''
        with open(sql_path, 'r') as file:
            next(file)
            next(file)
            attribute = file.readline().strip()

        return attribute

def main(sql_file_path, csv_file_path):
    attr = extract_dataset_attribute_from_sql(sql_file_path, csv_file_path)
    try:
        with open(csv_file_path, mode='r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)

            for line in csv_reader:
                # Assuming the line is a list of values, you might want to join them into a single string
                print(f'{line[attr]}\t{csv_file_path}:{line}')
    except FileNotFoundError:
        print(f"Error: CSV file '{csv_file_path}' not found.", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <sql_file_path> <csv_file_path>", file=sys.stderr)
        sys.exit(1)

    sql_file_path = sys.argv[1]
    csv_file_path = sys.argv[2]
    main(sql_file_path, csv_file_path)
