#!/bin/sh
cd ..
mvn clean compile assembly:single
cd - > /dev/null