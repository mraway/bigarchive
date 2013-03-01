#!/bin/bash

mode="release"
if [[ "$1" == "" ]]
then
    echo "no args given, run in release mode"
else
    echo "run in" $1 "mode"
    mode=$1
fi

../qfs/build/$mode/bin/chunkserver ../config/ChunkServer.prp ~/chunkserver.log > ~/chunkserver.out 2>&1 &