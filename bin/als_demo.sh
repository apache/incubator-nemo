#!/usr/bin/env bash

./bin/run.sh \
-job_id als \
-user_main edu.snu.onyx.examples.beam.AlternatingLeastSquare \
-optimization_policy edu.snu.onyx.compiler.optimizer.policy.DefaultPolicy \
-user_args "`pwd`/src/main/resources/sample_input_als 10 10"
