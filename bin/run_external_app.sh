#!/usr/bin/env bash
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd $parent_path
java -cp ../target/vortex-0.1-SNAPSHOT-shaded.jar:$1:`yarn classpath` edu.snu.vortex.client.JobLauncher "${@:2}"
