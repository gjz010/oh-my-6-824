#!/usr/bin/env bash
LOG_NAME=${$1:shardctrler.log}
counter=0
while time go test -failfast -race > $LOG_NAME ; do echo Test \#$counter success.; counter=$(($counter+1)); done
echo Fail at test $counter
