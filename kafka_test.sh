#!/bin/bash
<<<<<<< HEAD
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
=======

>>>>>>> 305b8d5af547c8a5ec6de25df219b12dcebd48f8

TIMEOUT=30
WINDOW=30
INTERVAL=30
EVENTS=0
PARALLELISM=2
PERIOD=50
CPU_DELAY=0
SAMPLING=1

ENABLE_OFFLOADING=false
<<<<<<< HEAD
ENABLE_OFFLOADING_DEBUG=true
POOL_SIZE=0
#POOL_SIZE=130
FLUSH_BYTES=$((10 * 1024 * 1024)) 
FLUSH_COUNT=10000
FLUSH_PERIOD=1000
EVENT_THRESHOLD=20000
MIN_VM_TASK=1

EXCLUDE_JARS="httpclient-4.2.5:httpcore-4.2.5:netty-:avro-"
EC2=true
=======
ENABLE_OFFLOADING_DEBUG=false
POOL_SIZE=2
#POOL_SIZE=170
FLUSH_BYTES=$((10 * 1024 * 1024))
FLUSH_COUNT=10000
FLUSH_PERIOD=1000

>>>>>>> 305b8d5af547c8a5ec6de25df219b12dcebd48f8

sourceType=KAFKA
pubSubMode=SUBSCRIBE_ONLY

<<<<<<< HEAD
echo run query $1 

./bin/run_nexmark.sh \
	-ec2 true \
        -job_id nexmark-Q$1 \
        -executor_json `pwd`/examples/resources/1.json \
=======
echo run query $1

./bin/run_nexmark.sh \
        -job_id nexmark-Q$1 \
        -executor_json `pwd`/examples/resources/beam_test_executor_resources.json \
>>>>>>> 305b8d5af547c8a5ec6de25df219b12dcebd48f8
        -user_main org.apache.beam.sdk.nexmark.Main \
  -optimization_policy org.apache.nemo.compiler.optimizer.policy.StreamingPolicy \
  -scheduler_impl_class_name org.apache.nemo.runtime.master.scheduler.StreamingScheduler \
        -enable_offloading $ENABLE_OFFLOADING \
        -enable_offloading_debug $ENABLE_OFFLOADING_DEBUG \
        -lambda_warmup_pool $POOL_SIZE \
        -flush_bytes $FLUSH_BYTES \
        -flush_count $FLUSH_COUNT \
        -flush_period $FLUSH_PERIOD \
<<<<<<< HEAD
	-exclude_jars $EXCLUDE_JARS \
	-source_parallelism $PARALLELISM \
	-is_local_source false \
	-event_threshold $EVENT_THRESHOLD \
	-min_vm_task $MIN_VM_TASK \
        -user_args "--runner=org.apache.nemo.client.beam.NemoRunner --streaming=true --query=$1 --manageResources=false --monitorJobs=true --streamTimeout=$TIMEOUT --numEventGenerators=$PARALLELISM --numEvents=$EVENTS --isRateLimited=true --windowSizeSec=$WINDOW --windowPeriodSec=$INTERVAL --fanout=1 --ratePeriodSec=$PERIOD --cpuDelayMs=$CPU_DELAY --samplingRate=$SAMPLING --sourceType=$sourceType --pubSubMode=$pubSubMode --bootstrapServers=13.113.69.217:9092"

=======
	      -is_local_source false \
        -user_args "--runner=org.apache.nemo.client.beam.NemoRunner --streaming=true --query=$1 --manageResources=false --monitorJobs=true --streamTimeout=$TIMEOUT --numEventGenerators=$PARALLELISM --numEvents=$EVENTS --isRateLimited=true  --windowSizeSec=$WINDOW --windowPeriodSec=$INTERVAL --fanout=1 --ratePeriodSec=$PERIOD --cpuDelayMs=$CPU_DELAY --samplingRate=$SAMPLING --sourceType=$sourceType --pubSubMode=$pubSubMode --bootstrapServers=kafka1:9092"
>>>>>>> 305b8d5af547c8a5ec6de25df219b12dcebd48f8
