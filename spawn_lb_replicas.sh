#!/bin/bash

python3.10 load_balancer.py &

python3.10 replica.py 5001 &
python3.10 replica.py 5002 &
python3.10 replica.py 5003 &
python3.10 replica.py 5004 &

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

wait
