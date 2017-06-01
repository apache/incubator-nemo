#!/usr/bin/env bash
java -cp target/vortex-0.1-SNAPSHOT-shaded.jar:`yarn classpath` edu.snu.vortex.client.JobLauncher "$@"
