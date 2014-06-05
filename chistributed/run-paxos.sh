#!/bin/bash
python broker.py -e "python paxos/node.py" -s "$1"
