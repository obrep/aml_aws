#!/bin/bash
set -x
sudo apt update
sudo apt install python3-pip -y
cd ../producer
pip install -r requirements.txt
python3 producer.py > log.txt
