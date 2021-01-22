#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information # regarding copyright ownership.  The ASF licenses this file
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

ENABLE_OFFLOADING_DEBUG=false
ENABLE_OFFLOADING=false

TIMEOUT=450

POOL_SIZE=0
#POOL_SIZE=130
FLUSH_BYTES=$((10 * 1024 * 1024)) 
FLUSH_COUNT=10000
FLUSH_PERIOD=1000
EVENT_THRESHOLD=20000
MIN_VM_TASK=1

EXCLUDE_JARS="httpclient-4.2.5:httpcore-4.2.5:netty-:avro-"
EC2=true

PARALLELISM=10

sourceType=KAFKA
pubSubMode=SUBSCRIBE_ONLY
WINDOW=10
INTERVAL=1
CPU_DELAY=1000
SAMPLING=0.1

echo run query $1 

./bin/run_nexmark.sh \
	-ec2 true \
        -job_id nexmark-Q$1 \
	-deploy_mode yarn \
        -executor_json `pwd`/examples/resources/1.json \
        -user_main org.apache.beam.sdk.nexmark.Main \
  -optimization_policy org.apache.nemo.compiler.optimizer.policy.StreamingPolicy \
  -scheduler_impl_class_name org.apache.nemo.runtime.master.scheduler.StreamingScheduler \
        -enable_offloading $ENABLE_OFFLOADING \
        -enable_offloading_debug $ENABLE_OFFLOADING_DEBUG \
        -lambda_warmup_pool $POOL_SIZE \
        -flush_bytes $FLUSH_BYTES \
        -flush_count $FLUSH_COUNT \
        -flush_period $FLUSH_PERIOD \
	-exclude_jars $EXCLUDE_JARS \
	-source_parallelism $PARALLELISM \
	-is_local_source false \
	-min_vm_task $MIN_VM_TASK \
        -user_args "--runner=org.apache.nemo.client.beam.NemoRunner --streaming=true --query=$1 --manageResources=false --monitorJobs=true --streamTimeout=$TIMEOUT --isRateLimited=true --windowSizeSec=$WINDOW --windowPeriodSec=$INTERVAL --fanout=1 --cpuDelayMs=$CPU_DELAY --samplingRate=$SAMPLING --sourceType=$sourceType --pubSubMode=$pubSubMode --bootstrapServers=broker1:9092"


#-user_args "--runner=org.apache.nemo.client.beam.NemoRunner --streaming=true --query=$1 --manageResources=false --monitorJobs=true --streamTimeout=$TIMEOUT --numEventGenerators=$PARALLELISM --numEvents=$EVENTS --isRateLimited=true --windowSizeSec=$WINDOW --windowPeriodSec=$INTERVAL --fanout=1 --ratePeriodSec=$PERIOD --cpuDelayMs=$CPU_DELAY --samplingRate=$SAMPLING --sourceType=$sourceType --pubSubMode=$pubSubMode --bootstrapServers=broker1:9092"

