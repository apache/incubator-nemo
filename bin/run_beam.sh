#!/usr/bin/env bash
#
# Copyright (C) 2018 Seoul National University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

java -Dlog4j.configuration=file://`pwd`/log4j.properties -cp examples/beam/target/nemo-examples-beam-0.1-SNAPSHOT-shaded.jar:`yarn classpath` edu.snu.nemo.client.JobLauncher "$@"
