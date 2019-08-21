#!/bin/sh
clear
echo $1
java $1 $2 -jar $(pwd)/../target/PeersPool-1.0-SNAPSHOT.jar
