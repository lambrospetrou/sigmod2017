#!/bin/bash

rm -f src/goimpl submission.tar.gz
/bin/tar czf submission.tar.gz package.sh run.sh compile.sh src/*.go
