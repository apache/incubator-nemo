/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.common.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Argument builder.
 */
public final class ArgBuilder {
  private static final List<List<String>> DEFAULT_ARGS = Arrays.asList(Arrays.asList("-executor_json",
      "../resources/sample_executor_resources.json"));
  private List<List<String>> args = new ArrayList<>();

  /**
   * @param jobId job id.
   * @return builder with the job id.
   */
  public ArgBuilder addJobId(final String jobId) {
    args.add(Arrays.asList("-job_id", jobId));
    return this;
  }

  /**
   * @param main user main class.
   * @return builder with the user main class.
   */
  public ArgBuilder addUserMain(final String main) {
    args.add(Arrays.asList("-user_main", main));
    return this;
  }

  /**
   * @param userArgs user arguments.
   * @return builder with the user arguments.
   */
  public ArgBuilder addUserArgs(final String... userArgs) {
    final StringJoiner joiner = new StringJoiner(" ");
    Arrays.stream(userArgs).forEach(joiner::add);
    args.add(Arrays.asList("-user_args", joiner.toString()));
    return this;
  }

  /**
   * @param policy optimization policy.
   * @return builder with the optimization policy.
   */
  public ArgBuilder addOptimizationPolicy(final String policy) {
    args.add(Arrays.asList("-optimization_policy", policy));
    return this;
  }

  /**
   * @param directory directory to save the DAG.
   * @return builder with the DAG directory.
   */
  public ArgBuilder addDAGDirectory(final String directory) {
    args.add(Arrays.asList("-dag_dir", directory));
    return this;
  }

  /**
   * @return the built arguments.
   */
  public String[] build() {
    // new String[0] is good for performance
    // see http://stackoverflow.com/questions/4042434/converting-arrayliststring-to-string-in-java
    return Stream.concat(args.stream().flatMap(List::stream), DEFAULT_ARGS.stream().flatMap(List::stream))
        .collect(Collectors.toList()).toArray(new String[0]);
  }
}
