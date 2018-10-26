package org.apache.nemo.common.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

/**
 * Argument builder.
 */
public final class ArgBuilder {
  private List<List<String>> args;

  /**
   * Constructor with default values.
   */
  public ArgBuilder() {
    this.args = new ArrayList<>();
  }

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

  public ArgBuilder addMaxTaskAttempt(final int maxAttempt) {
    args.add(Arrays.asList("-max_task_attempt", String.valueOf(maxAttempt)));
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
   * @param executorJsonFileName the name of the executor resource file to use.
   * @return builder with the executor resource file.
   */
  public ArgBuilder addResourceJson(final String executorJsonFileName) {
    args.add(Arrays.asList("-executor_json", executorJsonFileName));
    return this;
  }

  /**
   * @return the built arguments.
   */
  public String[] build() {
    return args.stream().flatMap(List::stream).toArray(String[]::new);
  }
}
