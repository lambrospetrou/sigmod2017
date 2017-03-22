#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

if [ "$(uname)" == "Darwin" ]; then
    pushd $DIR/src && make allmac ; popd
else
    pushd $DIR/src && make all ; popd
fi
