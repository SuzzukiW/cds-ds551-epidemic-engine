# test_mapper.py

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'word_count_python')))

from io import StringIO
from unittest.mock import patch
from mapper import main

def test_mapper_with_valid_input():
    input_data = "EVT0001,health_mention,2024-09-04 00:00:00,Chicago,Details for event 1\nEVT0002,general_health_report,2024-06-16 00:00:00,Tokyo,Details for event 2\n"
    expected_output = {
        "health_mention\t1",
        "Chicago\t1",
        "general_health_report\t1",
        "Tokyo\t1"
    }

    with patch('sys.stdin', StringIO(input_data)), patch('sys.stdout', new_callable=StringIO) as mock_stdout:
        main()
        actual_output = set(mock_stdout.getvalue().strip().split('\n'))
        assert actual_output == expected_output, f"Expected {expected_output}, but got {actual_output}"

def test_mapper_with_invalid_input():
    input_data = "1,Event1\n2,Event2,2023-05-02 11:00:00,Location2,Low,Details2\n"
    # removed the trailing newline from the expected output
    expected_output = "Event2\t1\nLocation2\t1"

    with patch('sys.stdin', StringIO(input_data)), patch('sys.stdout', new_callable=StringIO) as mock_stdout:
        main()
        # stripping the final output to remove trailing newlines
        actual_output = mock_stdout.getvalue().strip()
        assert actual_output == expected_output, f"Expected {expected_output!r}, but got {actual_output!r}"


def test_mapper_with_empty_input():
    input_data = ""
    expected_output = ""

    with patch('sys.stdin', StringIO(input_data)), patch('sys.stdout', new_callable=StringIO) as mock_stdout:
        main()
        actual_output = mock_stdout.getvalue().strip()
        assert actual_output == expected_output, f"Expected {expected_output}, but got {actual_output}"

