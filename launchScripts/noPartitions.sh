#!/bin/bash

pushd '../'
export case='case2'
gunicorn -w 4 -b 0.0.0.0:8000 load_balancer:app &
#python3.10 load_balancer.py &

python3.10 replica.py 5001 master 0 &

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

wait
