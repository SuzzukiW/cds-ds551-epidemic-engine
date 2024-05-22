#!/usr/bin/env python3
"""reducer.py"""

import sys

# input comes from STDIN (standard input)
# write some useful code here and print to STDOUT

def main():
    """ main function for reducer.py """
    data = {}

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        key, value = line.split('\t')

        if key in data:
            data[key] += 1
        else:
            data[key] = 1

    for key, value in data.items():
        print(f"{key}\t{value}")

if __name__ == "__main__":
    main()
