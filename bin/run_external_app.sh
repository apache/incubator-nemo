#!/usr/bin/env bash
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd $parent_path
java -cp examples/target/onyx-examples-0.1-SNAPSHOT-shaded.jar:$1:`yarn classpath` edu.snu.onyx.client.JobLauncher "${@:2}"
