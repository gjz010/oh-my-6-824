#!/usr/bin/env bash

while go test -failfast -run 2C -race | tee 2C-$1.log; do :; done
echo "Fail"
