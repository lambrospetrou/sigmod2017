#!/bin/bash

rm -f submission.tar.gz
tar czf submission.tar.gz package.sh run.sh compile.sh src/*.h src/*.hpp src/*.cpp src/Makefile src/include/*
