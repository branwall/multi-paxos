#!/bin/bash
python broker.py -e "python paxos/multi-node.py" -s "$1"
