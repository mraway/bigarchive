#!/bin/bash

pssh -i -h pssh_hosts memcached -d -m 512 -p 11985 -n 20
pssh -i -h pssh_hosts memcached -d -m 128 -p 11211 -n 20