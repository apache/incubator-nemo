#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one # or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# run this by ./bin/generate_javadocs.sh

TIMEOUT=840
WINDOW=40
INTERVAL=40
RATE=10
EVENTS=$((RATE*TIMEOUT))
PARALLELISM=1

echo run query $1

./bin/run_nexmark.sh \
        -job_id nexmark-Q$1 \
        -executor_json `pwd`/examples/resources/beam_test_executor_resources.json \
        -user_main org.apache.beam.sdk.nexmark.Main \
  -optimization_policy org.apache.nemo.compiler.optimizer.policy.StreamingPolicy \
  -scheduler_impl_class_name org.apache.nemo.runtime.master.scheduler.StreamingScheduler \
        -user_args "--runner=org.apache.nemo.client.beam.NemoRunner --streaming=true --query=$1 --manageResources=false --monitorJobs=true --streamTimeout=$TIMEOUT --numEventGenerators=$PARALLELISM --numEvents=$EVENTS --isRateLimited=true --firstEventRate=$RATE --nextEventRate=$RATE --windowSizeSec=$WINDOW --windowPeriodSec=$INTERVAL --fanout=1 --samplingRate=1.0"