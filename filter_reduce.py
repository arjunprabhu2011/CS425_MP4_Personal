import sys

def main(sdfs_intermediary_file):
    with open(sdfs_intermediary_file, 'r') as file:
        for line in file:
            print(f"{line},done")

if __name__ == "__main__":
    sdfs_intermediary_path = sys.argv[1]
    main(sdfs_intermediary_path)
