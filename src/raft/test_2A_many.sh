#!/usr/bin/env bash

while go test -failfast -run 2A -race | tee 2A-$1.log ; do :; done
echo "Fail"
