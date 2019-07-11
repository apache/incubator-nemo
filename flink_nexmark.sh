#!/bin/bash
./gradlew :sdks:java:testing:nexmark:run \
    -Pnexmark.runner=":runners:flink:1.5" \
    -Pnexmark.args="
        --runner=FlinkRunner
        --suite=SMOKE
        --streamTimeout=60
        --streaming=false
        --manageResources=false
        --monitorJobs=true
        --flinkMaster=[local]"
