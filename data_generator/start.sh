#!/bin/sh

# create topic
python3 create_topic.py

# get and unpack data
# wget https://archive.ics.uci.edu/static/public/352/online+retail.zip
unzip online+retail.zip

# produce data
python3 produce.py
