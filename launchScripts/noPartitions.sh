#!/bin/bash

gunicorn -w 4 -b 0.0.0.0:8000 load_balancer:app &
python ../load_balancer.py &

python ../replica.py 5001 master 0 &

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

wait