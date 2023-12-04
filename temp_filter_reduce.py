import sys

def main():
    for line in sys.stdin:
        _, value = line.strip().split(',', 1)
        print(f"{value},done")

if __name__ == "__main__":
    main()
