#!/usr/bin/env bash

while go test -failfast -run 2D -race | tee 2D-$1.log; do :; done
echo "Fail"
