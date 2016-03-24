#!/bin/bash

# Assumes that this repo is checked out at /home/ubuntu/error-monitor-db

add-apt-repository 'deb https://cran.cnr.berkeley.edu/bin/linux/ubuntu trusty/'
apt-key adv --keyserver keyserver.ubuntu.com --recv-keys '51716619E084DAB9'
apt-get update
apt-get -y install libcurl4-openssl-dev libssl-dev redis-server python-pip python-dev python-numpy python-scipy r-base r-base-dev r-recommended
Rscript setup.R
pip install -r requirements.txt

cp /home/ubuntu/error-monitor-db/server.conf /etc/init/error-monitor-db.conf
chown root:root /etc/init/error-monitor-db.conf
chmod 644 /etc/init/error-monitor-db.conf

start error-monitor-db
