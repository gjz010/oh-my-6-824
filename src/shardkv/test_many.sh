#!/usr/bin/env bash
counter=0
while time go test -failfast -run "TestStaticShards|TestJoinLeave|TestSnapshot|TestMissChange|TestConcurrent|TestUnreliable" ; do echo Test \#$counter success.; counter=$(($counter+1)); done
echo Fail at test $counter
