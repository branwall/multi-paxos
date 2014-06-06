#!/bin/bash
python broker.py -e "python paxos/synod-node.py" -s "$1"
