#!/bin/bash

rm jars/*
rm build/distributions/*
rm build/libs/*
./copy_libraries.sh


if [ "$#" -lt 1 ]; then
  echo "enter the number of lambda"
  return
fi


NUM=$1

echo "generate $NUM functions"
./generate_functions.sh $NUM

gradle build
sls deploy
