import sys

def main(sdfs_intermediary_file):
    with open(sdfs_intermediary_file, 'r') as file:
        for line in file:
            detection, count, total = line.strip().split(',')
            percentage = (int(count) / int(total)) * 100 if total != '0' else 0
            print(f"Percentage of '{detection}': {percentage:.2f}%")

# def main():
#     for line in sys.stdin:
#         print(line)
#         detection, count, total = line.split(',')
#         percentage = (int(count) / int(total)) * 100 if total != '0' else 0
#         print(f"Percentage of '{detection}': {percentage:.2f}%,done")

if __name__ == "__main__":
    sdfs_intermediary_path = sys.argv[1]
    main(sdfs_intermediary_path)
    # main()