# test_reducer.py

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'word_count_python')))

import sys
from io import StringIO
from unittest.mock import patch
from reducer import main

def test_reducer_with_valid_input():
    input_data = "Event1\t1\nEvent1\t1\nLocation1\t1\nLocation2\t1\nEvent2\t1\n"
    expected_output = "Event1\t2\nLocation1\t1\nLocation2\t1\nEvent2\t1\n"

    with patch('sys.stdin', StringIO(input_data)), patch('sys.stdout', new_callable=StringIO) as mock_stdout:
        main()
        assert mock_stdout.getvalue() == expected_output

def test_reducer_with_empty_input():
    input_data = ""
    expected_output = ""

    with patch('sys.stdin', StringIO(input_data)), patch('sys.stdout', new_callable=StringIO) as mock_stdout:
        main()
        assert mock_stdout.getvalue() == expected_output