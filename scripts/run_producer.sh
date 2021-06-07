#!/bin/bash
set -x
sudo apt install python3-pip -y
cd ../producer
pip install -r requirements.txt
