import sys
from collections import defaultdict

def main(sdfs_intermediary_file):
    detection_counts = defaultdict(int)
    total_count = 0
    with open(intermediate_file, 'r') as file:
        for line in file:
            key, value = line.strip().split(', ')
            if key != "Total":
                detection_counts[key] += int(value)
            else:
                total_count += int(value)
    
    # Print the output for the next stage
    for detection, count in detection_counts.items():
        print(f"{detection},{count},{total_count}")

# def main():
#     detection_counts = defaultdict(int)
#     total_count = 0

#     for line in sys.stdin:
#         key, value = line.strip().split(',', 1)
#         if key != "Total":
#             detection_counts[key] += int(value)
#         else:
#             total_count += int(value)
    
#     # Print the output for the next stage
#     for detection, count in detection_counts.items():
#         print(f"{detection},{count},{total_count}")

if __name__ == "__main__":
    sdfs_intermediary_path = sys.argv[1]
    main(sdfs_intermediary_path)
    # main()