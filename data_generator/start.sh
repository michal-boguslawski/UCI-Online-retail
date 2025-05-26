#!/bin/sh

# get and unpack data
wget https://archive.ics.uci.edu/static/public/352/online+retail.zip
unzip online+retail.zip

# create topic
python3 create_topic.py

# produce data
python3 produce.py