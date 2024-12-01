#!/bin/bash

pushd '../'
export case='case1'
gunicorn -w 4 -b 0.0.0.0:8000 load_balancer:app&

python3.10 replica.py 5001 master 0 &
python3.10 replica.py 5002 master 0 &
python3.10 replica.py 5003 master 0 &

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

wait
