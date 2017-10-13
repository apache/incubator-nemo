#!/usr/bin/env bash

./bin/run.sh \
-job_id mr_demo_push \
-user_main edu.snu.onyx.examples.beam.MapReduce \
-optimization_policy edu.snu.onyx.compiler.optimizer.policy.PushMemoryMRPolicy \
-user_args "`pwd`/src/main/resources/sample_input_mr `pwd`/src/main/resources/sample_output_push"
