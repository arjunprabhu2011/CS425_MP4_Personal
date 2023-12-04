def main(sql_file_path, input_file_path):
    total_count = 0
    seen_total = 0
    with open(input_file_path, 'r') as file:
        for line in file:
            if "Total" in line and seen_total == 0:
                parts = text.split(',')
                # Convert the second part to an integer
                total_count = int(parts[1])
                seen_total = 1

    with open(input_file_path, 'r') as file:
        for line in file:
            if "Total" not in line:
                print(f"{line},{total_count}")

# def main():
#     with open(csv_file_path, 'r') as file:
#         for line in file:
#             print(line)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <sql_file_path> <csv_file_path>", file=sys.stderr)
        sys.exit(1)

    sql_file_path = sys.argv[1]
    input_file_path = sys.argv[2]
    main(sql_file_path, input_file_path)
    # main()