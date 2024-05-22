#!/usr/bin/env python3
"""mapper.py"""

import sys

# input comes from STDIN (standard input)
# write some useful code here and print to STDOUT

def main():
    import sys
    input_stream = sys.stdin
    
    for line in input_stream:
        line = line.strip()
        if line:  # ensures that non-empty lines are processed
            parts = line.split(',')
            if len(parts) >= 4:  # a validation to ensure there are enough parts
                event_type = parts[1]
                location = parts[3]
                print(f"{event_type}\t1")
                print(f"{location}\t1")

if __name__ == "__main__":
    main()

