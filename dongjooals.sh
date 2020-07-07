#!/usr/bin/env bash
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


./bin/run_beam.sh \
    -job_id als \
    -executor_json `pwd`/examples/resources/executors/beam_test_executor_resources.json \
    -user_main org.apache.nemo.examples.beam.AlternatingLeastSquare \
    -optimization_policy org.apache.nemo.compiler.optimizer.policy.DefaultPolicy \
    -user_args "`pwd`/examples/resources/inputs/test_input_als 10 3 0.05 `pwd`/outputs/outputs-als"


cp ./REEF_LOCAL_RUNTIME/als-*/dag/plan-logical.json ~/Desktop/begin.json
cp ./REEF_LOCAL_RUNTIME/als-*/dag/Plan*-final.json ~/Desktop/final.json

