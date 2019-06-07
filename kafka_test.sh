#!/bin/bash


TIMEOUT=30
WINDOW=30
INTERVAL=30
EVENTS=0
PARALLELISM=2
PERIOD=50
CPU_DELAY=0
SAMPLING=1

ENABLE_OFFLOADING=false
ENABLE_OFFLOADING_DEBUG=false
POOL_SIZE=2
#POOL_SIZE=170
FLUSH_BYTES=$((10 * 1024 * 1024))
FLUSH_COUNT=10000
FLUSH_PERIOD=1000


sourceType=KAFKA
pubSubMode=SUBSCRIBE_ONLY

echo run query $1

./bin/run_nexmark.sh \
        -job_id nexmark-Q$1 \
        -executor_json `pwd`/examples/resources/beam_test_executor_resources.json \
        -user_main org.apache.beam.sdk.nexmark.Main \
  -optimization_policy org.apache.nemo.compiler.optimizer.policy.StreamingPolicy \
  -scheduler_impl_class_name org.apache.nemo.runtime.master.scheduler.StreamingScheduler \
        -enable_offloading $ENABLE_OFFLOADING \
        -enable_offloading_debug $ENABLE_OFFLOADING_DEBUG \
        -lambda_warmup_pool $POOL_SIZE \
        -flush_bytes $FLUSH_BYTES \
        -flush_count $FLUSH_COUNT \
        -flush_period $FLUSH_PERIOD \
	      -is_local_source false \
        -user_args "--runner=org.apache.nemo.client.beam.NemoRunner --streaming=true --query=$1 --manageResources=false --monitorJobs=true --streamTimeout=$TIMEOUT --numEventGenerators=$PARALLELISM --numEvents=$EVENTS --isRateLimited=true  --windowSizeSec=$WINDOW --windowPeriodSec=$INTERVAL --fanout=1 --ratePeriodSec=$PERIOD --cpuDelayMs=$CPU_DELAY --samplingRate=$SAMPLING --sourceType=$sourceType --pubSubMode=$pubSubMode --bootstrapServers=kafka1:9092"
