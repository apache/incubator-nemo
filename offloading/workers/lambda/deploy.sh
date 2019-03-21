#!/bin/bash

rm jars/*
rm build/distributions/*
rm build/libs/*
./copy_libraries.sh
gradle build
sls deploy
