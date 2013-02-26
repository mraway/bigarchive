#!/bin/bash

/home/qfs/build/$1/bin/chunkserver /home/bigarchive/config/ChunkServer.prp ~/chunkserver.log > ~/chunkserver.out 2>&1 &