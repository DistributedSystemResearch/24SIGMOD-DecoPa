#!/bin/bash
python3 send_eventstream.py 0 > 0.log &
python3 send_eventstream.py 1 > 1.log &
python3 send_eventstream.py 2 > 2.log &
