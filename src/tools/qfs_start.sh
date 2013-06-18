#!/bin/bash

pssh -i -h ../config/pssh_hosts /home/bigarchive/bin/chunkserver_start.sh

echo "This program only starts chunkservers, you need to start metaserver manually."
