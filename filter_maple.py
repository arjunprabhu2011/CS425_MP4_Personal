import sys
import re
import csv

def extract_regex_from_sql(sql_query):
    regex_match = re.search(r"WHERE\s+\"([^\"]+)\"", sql_query)
    regex = regex_match.group(1) if regex_match else ""

    print(regex + "\n")

    return regex

def filter_line(line, regex):
    if re.search(regex, line):
        return line
    return None

def main(sql_file_path, csv_file_path):
    try:
        with open(sql_file_path, 'r') as sql_file:
            sql_query = sql_file.read()

            print("SQL: " + sql_query)
    except FileNotFoundError:
        print(f"Error: SQL file '{sql_file_path}' not found.", file=sys.stderr)
        sys.exit(1)

    print("SQL_2: " + sql_query)
    regex = extract_regex_from_sql(sql_query)

    try:
        with open(csv_file_path, mode='r', encoding='utf-8') as file:
            reader = csv.reader(file)
            header = next(reader)
            line_count = 0
            for line in reader:
                # Assuming the line is a list of values, you might want to join them into a single string
                line_str = ','.join(line)
                filtered_line = filter_line(line_str, regex)
                if filtered_line:
                    key = line_count % 100
                    print(f"{key},{filtered_line}")
                    line_count += 1
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
