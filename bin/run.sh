#!/usr/bin/env bash
java -cp target/onyx-0.1-SNAPSHOT-shaded.jar:`yarn classpath` edu.snu.onyx.client.JobLauncher "$@"
