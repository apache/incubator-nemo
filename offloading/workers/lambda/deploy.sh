#!/bin/bash

rm jars/*
./copy_libraries.sh
gradle build
sls deploy
