package edu.snu.vortex.client;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 * Vortex Job Configurations.
 */
public final class JobConf extends ConfigurationModuleBuilder {

  //////////////////////////////// User Configurations

  /**
   * Job id.
   */
  @NamedParameter(doc = "Job id", short_name = "job_id")
  public final class JobId implements Name<String> {
  }

  /**
   * User Main Class Name.
   */
  @NamedParameter(doc = "User Main Class Name", short_name = "user_main")
  public final class UserMainClass implements Name<String> {
  }

  /**
   * User Main Arguments.
   */
  @NamedParameter(doc = "User Main Arguments", short_name = "user_args")
  public final class UserMainArguments implements Name<String> {
  }

  /**
   * Directory to store JSON representation of intermediate DAGs.
   */
  @NamedParameter(doc = "Directory to store intermediate DAGs", short_name = "dag_dir", default_value = "./dag")
  public final class DAGDirectory implements Name<String> {
  }

  /**
   * Directory to store files for storing blocks.
   */
  @NamedParameter(doc = "Directory to store files", short_name = "file_dir", default_value = "./files")
  public final class FileDirectory implements Name<String> {
  }

  //////////////////////////////// Compiler Configurations

  /**
   * Name of the optimization policy.
   */
  @NamedParameter(doc = "Name of the optimization policy", short_name = "optimization_policy",
      default_value = "default")
  public final class OptimizationPolicy implements Name<String> {
  }

  //////////////////////////////// Runtime Configurations

  /**
   * Deploy mode.
   */
  @NamedParameter(doc = "Deploy mode", short_name = "deploy_mode", default_value = "local")
  public final class DeployMode implements Name<String> {
  }

  /**
   * Vortex driver memory.
   */
  @NamedParameter(doc = "Vortex driver memory", short_name = "driver_mem_mb", default_value = "1024")
  public final class DriverMemMb implements Name<Integer> {
  }

  /**
   * Path to the JSON file that specifies resource layout.
   */
  @NamedParameter(doc = "Path to the JSON file that specifies resources for executors", short_name = "executor_json",
  default_value = "default.json")
  public final class ExecutorJsonPath implements Name<String> {
  }

  /**
   * Contents of the JSON file that specifies resource layout.
   */
  @NamedParameter(doc = "Contents of JSON file that specifies resources for executors")
  public final class ExecutorJsonContents implements Name<String> {
  }

  /**
   * VortexExecutor capacity.
   * Determines the number of TaskGroup 'slots' for each executor.
   * 1) Master's TaskGroup scheduler can use this number in scheduling.
   *    (e.g., schedule TaskGroup to the executor currently with the maximum number of available slots)
   * 2) Executor's number of TaskGroup execution threads is set to this number.
   */
  @NamedParameter(doc = "VortexExecutor capacity", short_name = "executor_capacity", default_value = "1")
  public final class ExecutorCapacity implements Name<Integer> {
  }

  /**
   * Scheduler timeout in ms.
   */
  @NamedParameter(doc = "Scheduler timeout in ms", short_name = "scheduler_timeout_ms", default_value = "10000")
  public final class SchedulerTimeoutMs implements Name<Integer> {
  }

  /**
   * VortexExecutor id.
   */
  @NamedParameter(doc = "Executor id", short_name = "executor_id")
  public final class ExecutorId implements Name<String> {
  }

  /**
   * Max number of attempts for task group scheduling.
   */
  @NamedParameter(doc = "Max number of schedules", short_name = "max_schedule_attempt", default_value = "3")
  public final class MaxScheduleAttempt implements Name<Integer> {
  }

  /**
   * Block size.
   */
  @NamedParameter(doc = "Block size (in KB)", short_name = "block_size", default_value = "128000")
  public final class BlockSize implements Name<Integer> {
  }

  /**
   * Hash range multiplier.
   * If we need to split or recombine an output data from a task after it is stored,
   * we multiply the hash range with this factor in advance
   * to prevent the extra deserialize - rehash - serialize process.
   * In these cases, the hash range will be (hash range multiplier X destination task parallelism).
   * The reason why we do not divide the output into a fixed number is that the fixed number can be smaller than
   * the destination task parallelism.
   */
  @NamedParameter(doc = "Hash range multiplier", short_name = "hash_range_multiplier", default_value = "10")
  public final class HashRangeMultiplier implements Name<Integer> {
  }

  public static final RequiredParameter<String> EXECUTOR_ID = new RequiredParameter<>();
  public static final OptionalParameter<Integer> EXECUTOR_CAPACITY = new OptionalParameter<>();

  public static final ConfigurationModule EXECUTOR_CONF = new JobConf()
      .bindNamedParameter(ExecutorId.class, EXECUTOR_ID)
      .bindNamedParameter(ExecutorCapacity.class, EXECUTOR_CAPACITY)
      .build();

}
