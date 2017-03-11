#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

NUM_THREADS=$( grep -e "siblings" /proc/cpuinfo | sed 's/^.*: \([0-9]\+\)$/\1/' | head -n1 )
NUM_THREADS=2

# OpenMP configuration
export OMP_NUM_THREADS="$NUM_THREADS"
#export OMP_PROC_BIND=TRUE
#export OMP_WAIT_POLICY=ACTIVE
#export OMP_WAIT_POLICY=ACTIVE

$DIR/src/main "$OMP_NUM_THREADS"
