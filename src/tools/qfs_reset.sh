#!/bin/bash

pssh -i -h ../config/pssh_hosts rm -rf /mnt/data1/qfs/*
pssh -i -h ../config/pssh_hosts rm -rf ~/chunkserver.log*
pssh -i -h ../config/pssh_hosts rm -rf ~/chunkserver.out*
pssh -i -h ../config/pssh_hosts rm -rf ~/metaserver.log*
pssh -i -h ../config/pssh_hosts rm -rf ~/metaserver.out*
pssh -i -h ../config/pssh_hosts rm -rf /home/bgtest/qfs_log/*
pssh -i -h ../config/pssh_hosts rm -rf /home/bgtest/qfs_checkpoint/*
