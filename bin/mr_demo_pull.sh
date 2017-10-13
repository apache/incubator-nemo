#!/usr/bin/env bash

./bin/run.sh \
-job_id mr_demo_pull \
-user_main edu.snu.vortex.examples.beam.MapReduce \
-optimization_policy edu.snu.vortex.compiler.optimizer.policy.PullDiskMRPolicy \
-user_args "`pwd`/src/main/resources/sample_input_mr `pwd`/src/main/resources/sample_output_pull"
