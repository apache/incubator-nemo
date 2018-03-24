#!/usr/bin/env bash

echo "You should already have SonarQube installed and running at localhost:9000"
echo "e.g. OSX: brew install sonarqube && sonar console"
sonar console
mvn clean package sonar:sonar
