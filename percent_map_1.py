import sys
import re
import csv

def extract_regex_from_sql(sql_query):
    regex_match = re.search(r"WHERE\s+Interconne\s*=\s*\"([^\"]+)\"", sql_query)
    regex = regex_match.group(1) if regex_match else ""

    return regex

def main(sql_file_path, csv_file_path):
    try:
        with open(sql_file_path, 'r') as sql_file:
            sql_query = sql_file.read()

    except FileNotFoundError:
        print(f"Error: SQL file '{sql_file_path}' not found.", file=sys.stderr)
        sys.exit(1)

    interconne_type = extract_regex_from_sql(sql_query)
    with open(csv_file_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row['Interconne'] == interconne_type:
                with open("tester.txt", 'w') as f:
                    f.write("HERREEEEE")
                    f.write(csv_file_path)
                    f.write("HOWDY")

                detection_value = row['Detection_']
                if detection_value != "":
                    print(f"{detection_value},1")
                    print("Total,1")
                else:
                    print("empty_string,1")
                    print("Total,1")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <sql_file_path> <csv_file_path>", file=sys.stderr)
        sys.exit(1)

    sql_file_path = sys.argv[1]
    csv_file_path = sys.argv[2]
    main(sql_file_path, csv_file_path)
