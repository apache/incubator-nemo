#!/usr/bin/env bash

echo "You should already have SonarQube installed and running at localhost:9000"
echo "e.g. brew install sonarqube && sonar console"
mvn clean package sonar:sonar