#!/bin/bash

mode="release"
if [[ "$1" == "" ]]
then
    echo "no args given, run in release mode"
else
    echo "run in" $1 "mode"
    mode=$1
fi

../qfs/build/$mode/bin/metaserver -c ../config/MetaServer.prp ~/metaserver.log > ~/metaserver.out 2>&1 &