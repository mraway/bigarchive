#!/bin/bash

pssh -i -h ../config/pssh_hosts killall chunkserver
pssh -i -h ../config/pssh_hosts killall metaserver