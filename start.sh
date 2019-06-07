#!/bin/bash
# 
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
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

TIMEOUT=30
WINDOW=30
INTERVAL=30
EVENTS=0
PARALLELISM=1
PERIOD=50
NORMAL=10
BURSTY=10
CPU_DELAY=0
SAMPLING=0.9

ENABLE_OFFLOADING=false
ENABLE_OFFLOADING_DEBUG=false
POOL_SIZE=0
FLUSH_BYTES=$((10 * 1024 * 1024))
FLUSH_COUNT=10

./bin/run_nexmark.sh \
 	-job_id nexmark-Q0 \
	-executor_json `pwd`/examples/resources/executors/beam_test_executor_resources.json \
 	-user_main org.apache.beam.sdk.nexmark.Main \
 	-optimization_policy org.apache.nemo.compiler.optimizer.policy.StreamingPolicy \
	-scheduler_impl_class_name org.apache.nemo.runtime.master.scheduler.StreamingScheduler \
        -user_args "--runner=org.apache.nemo.client.beam.NemoRunner --streaming=true --query=$1 --manageResources=false --monitorJobs=true --streamTimeout=$TIMEOUT"
