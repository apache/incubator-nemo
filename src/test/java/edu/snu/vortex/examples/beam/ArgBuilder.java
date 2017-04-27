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
package edu.snu.vortex.examples.beam;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class ArgBuilder {
  private List<List<String>> args = new ArrayList<>();

  public ArgBuilder addUserMain(final String main) {
    args.add(Arrays.asList("-user_main", main));
    return this;
  }

  public ArgBuilder addUserArgs(final String... userArgs) {
    final StringJoiner joiner = new StringJoiner(" ");
    Arrays.stream(userArgs).forEach(joiner::add);
    args.add(Arrays.asList("-user_args", joiner.toString()));
    return this;
  }

  public ArgBuilder addOptimizationPolicy(final String policy) {
    args.add(Arrays.asList("-optimization_policy", policy));
    return this;
  }

  public ArgBuilder addDAGDirectory(final String directory) {
    args.add(Arrays.asList("-dag_dir", directory));
    return this;
  }

  public String[] build() {
    // new String[0] is good for performance
    // see http://stackoverflow.com/questions/4042434/converting-arrayliststring-to-string-in-java
    return args.stream().flatMap(List::stream).collect(Collectors.toList()).toArray(new String[0]);
  }
}
