#!/usr/bin/env bash
java -cp examples/target/onyx-examples-0.1-SNAPSHOT-shaded.jar:`yarn classpath` edu.snu.onyx.client.JobLauncher "$@"
