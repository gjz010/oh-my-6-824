#!/usr/bin/env bash
counter=0
while time go test -failfast > kvraft.log ; do echo Test \#$counter success.; counter=$(($counter+1)); done
echo Fail at test $counter
