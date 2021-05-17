#!/usr/bin/env bash

while go test -failfast -run 2B -race | tee 2B-$1.log ; do :; done
echo "Fail"
