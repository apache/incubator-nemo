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

TIMEOUT=450
WINDOW=5
INTERVAL=5
EVENTS=0
PARALLELISM=4
EXECUTOR_THREADS=2
PERIOD=50
NORMAL=10
BURSTY=10
CPU_DELAY=0
SAMPLING=1

ENABLE_OFFLOADING=false
ENABLE_OFFLOADING_DEBUG=false
POOL_SIZE=0
FLUSH_BYTES=$((10 * 1024 * 1024)) 
FLUSH_COUNT=10000
FLUSH_PERIOD=1000
EXCLUDE_JARS="httpclient-4.2.5:httpcore:netty-:avro:slf4j"

echo run query $1

./bin/run_nexmark.sh \
        -ec2 false \
        -job_id nexmark-Q$1 \
        -executor_json `pwd`/examples/resources/1.json \
        -user_main org.apache.beam.sdk.nexmark.Main \
        -optimization_policy org.apache.nemo.compiler.optimizer.policy.StreamingPolicy \
  -scheduler_impl_class_name org.apache.nemo.runtime.master.scheduler.StreamingScheduler \
        -sampling_path sampling.json \
        -enable_offloading $ENABLE_OFFLOADING \
        -enable_offloading_debug $ENABLE_OFFLOADING_DEBUG \
        -executor_threads $EXECUTOR_THREADS \
        -lambda_warmup_pool $POOL_SIZE \
        -source_parallelism $PARALLELISM \
        -is_local_source true \
        -flush_bytes $FLUSH_BYTES \
        -flush_count $FLUSH_COUNT \
        -flush_period $FLUSH_PERIOD \
        -exclude_jars $EXCLUDE_JARS \
        -user_args "--runner=org.apache.nemo.client.beam.NemoRunner --streaming=true --query=$1 --manageResources=false --monitorJobs=true --streamTimeout=$TIMEOUT --numEventGenerators=$PARALLELISM --numEvents=$EVENTS --isRateLimited=true --firstEventRate=$NORMAL --nextEventRate=$BURSTY --windowSizeSec=$WINDOW --windowPeriodSec=$INTERVAL --fanout=1 --rateShape=BURSTY --ratePeriodSec=$PERIOD --auctionSkip=1 --cpuDelayMs=$CPU_DELAY --samplingRate=$SAMPLING"
