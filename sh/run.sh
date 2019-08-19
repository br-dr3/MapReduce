#!/bin/sh
clear
java -Did=$1 -Dpath=$2 -jar $(pwd)/../target/PeersPool-1.0-SNAPSHOT.jar
