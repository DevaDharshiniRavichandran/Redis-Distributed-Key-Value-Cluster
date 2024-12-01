#!/bin/bash

pushd '../'
export case='case3'
gunicorn -w 4 -b 0.0.0.0:8000 load_balancer:app &
#python ../load_balancer.py &

python3.10 replica.py 5001 master 0 &
python3.10 replica.py 5002 slave 5001 &
python3.10 replica.py 5003 slave 5001 &

python3.10 replica.py 5004 master 0 &
python3.10 replica.py 5005 slave 5004 &
python3.10 replica.py 5006 slave 5004 &

python3.10 replica.py 5007 master 0 &
python3.10 replica.py 5008 slave 5007 &
python3.10 replica.py 5009 slave 5007 &

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

wait
