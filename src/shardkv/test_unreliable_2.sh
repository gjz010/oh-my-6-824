#!/usr/bin/env bash
LOG_NAME=$1
LOG_NAME=${LOG_NAME:="shardkv.log"}
counter=0
while time go test  -timeout 1200s -failfast -run "TestUnreliable2"  > $LOG_NAME ; do echo Test \#$counter success.; counter=$(($counter+1)); done
#while time go test -failfast -run "TestSnapshot" -race > $LOG_NAME ; do echo Test \#$counter success.; counter=$(($counter+1)); done
echo Fail at test $counter
