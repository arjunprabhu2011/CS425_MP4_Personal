import sys

def main(sdfs_intermediary_file):
    first_dataset = ''
    key_to_lines_map = {}

    with open(sdfs_intermediary_file, 'r') as file:
        for line in file:
            for line in sys.stdin:
                line = line.strip()
                key, value = line.split(',', 1)
                source_dataset, record = value.split(':', 1)

                if first_dataset == '':
                    first_dataset = source_dataset

                if source_dataset == first_dataset:
                    if key not in key_to_lines_map:
                        key_to_lines_map[key] = ([record], [])
                    else:
                        key_to_lines_map[key][0].append(record)
                else:
                    if key not in key_to_lines_map:
                        key_to_lines_map[key] = ([], [record])
                    else:
                        key_to_lines_map[key][1].append(record)

    for k in key_to_lines_map:
        if (len(key_to_lines_map[k][0]) == 0 or len(key_to_lines_map[k][1]) == 0):
            continue
        else:
            for v1 in key_to_lines_map[k][0]:
                for v2 in key_to_lines_map[k][1]:
                    print(f'{k},{v1};{v2}')

if __name__ == "__main__":
    sdfs_intermediary_path = sys.argv[1]
    main(sdfs_intermediary_path)

        