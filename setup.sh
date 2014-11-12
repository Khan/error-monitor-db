#!/bin/bash

# Assumes that this repo is checked out at /home/ubuntu/error-monitor-db

apt-get update
apt-get -y install redis-server python-pip python-dev python-numpy python-scipy
pip install -r requirements.txt

cp /home/ubuntu/error-monitor-db/server.conf /etc/init/error-monitor-db.conf
chown root:root /etc/init/error-monitor-db.conf
chmod 644 /etc/init/error-monitor-db.conf

start error-monitor-db
