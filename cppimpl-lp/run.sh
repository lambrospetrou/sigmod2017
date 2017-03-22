#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

#NUM_THREADS=$( grep -e "siblings" /proc/cpuinfo | sed 's/^.*: \([0-9]\+\)$/\1/' | head -n1 )
NUM_THREADS=1

#THREAD_AFFINITIES="0,2,"
THREAD_AFFINITIES=""
i=0
while [ "$i" -lt "$NUM_THREADS" ]; do
    TA="$TA$(($i * 2)),"
    i=$(($i + 1))
done
THREAD_AFFINITIES=$( echo "$TA" | sed 's/\(.*\)\,$/\1/' )
#echo $THREAD_AFFINITIES

# OpenMP configuration
export OMP_NUM_THREADS="$NUM_THREADS"
export OMP_PROC_BIND=TRUE
export GOMP_CPU_AFFINITY="$THREAD_AFFINITIES"

export OMP_WAIT_POLICY=ACTIVE
#export OMP_WAIT_POLICY=PASSIVE

$DIR/src/main "$OMP_NUM_THREADS"
