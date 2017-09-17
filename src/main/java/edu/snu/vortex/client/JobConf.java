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

  /**
   * Directory points the mounted GlusterFS volume to store files in remote fashion.
   * If the volume is not mounted to this directory, the remote file store will act like local file store
   * (but maybe inefficiently).
   */
  @NamedParameter(doc = "Directory points the GlusterFS volume", short_name = "gfs_dir", default_value = "../tmp_gfs")
  public final class GlusterVolumeDirectory implements Name<String> {
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
  default_value = "config/default.json")
  public final class ExecutorJsonPath implements Name<String> {
  }

  /**
   * The fraction of container memory not to use fo the JVM heap.
   */
  @NamedParameter(doc = "The fraction of the container memory not to use for the JVM heap", short_name = "heap_slack",
      default_value = "0.3")
  public final class JVMHeapSlack implements Name<Double> {
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
   * Number of I/O threads for {@link edu.snu.vortex.runtime.executor.data.LocalFileStore}.
   */
  @NamedParameter(doc = "Number of I/O threads for LocalFileStore", short_name = "local_file_threads",
      default_value = "5")
  public final class LocalFileStoreNumThreads implements Name<Integer> {
  }

  /**
   * Number of I/O threads for {@link edu.snu.vortex.runtime.executor.data.GlusterFileStore}.
   */
  @NamedParameter(doc = "Number of I/O threads for GlusterFileStore", short_name = "gluster_file_threads",
      default_value = "5")
  public final class GlusterFileStoreNumThreads implements Name<Integer> {
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

  /**
   * The number of threads in thread pool for inbound
   * {@link edu.snu.vortex.runtime.executor.data.partitiontransfer.PartitionTransfer}.
   *
   * These threads are responsible for de-serializing bytes into partition.
   */
  @NamedParameter(doc = "Number of threads for inbound partition transfer", short_name = "partition_threads_inbound",
      default_value = "5")
  public final class PartitionTransferInboundNumThreads implements Name<Integer> {
  }

  /**
   * The number of threads in thread pool for outbound
   * {@link edu.snu.vortex.runtime.executor.data.partitiontransfer.PartitionTransfer}.
   *
   * These threads are responsible for serializing partition into bytes.
   */
  @NamedParameter(doc = "Number of threads for outbound partition transfer", short_name = "partition_threads_outbound",
      default_value = "5")
  public final class PartitionTransferOutboundNumThreads implements Name<Integer> {
  }

  /**
   * The size of outbound buffers for {@link edu.snu.vortex.runtime.executor.data.partitiontransfer.PartitionTransfer},
   * in bytes.
   */
  @NamedParameter(doc = "Size of outbound buffers for partition transfer, in bytes",
      short_name = "partition_outbound_buffer", default_value = "10485760")
  public final class PartitionTransferOutboundBufferSize implements Name<Integer> {
  }

  /**
   * The TCP port to which local
   * {@link edu.snu.vortex.runtime.executor.data.partitiontransfer.PartitionTransport} binds. 0 means random port.
   */
  @NamedParameter(doc = "Port to which PartitionTransport binds (0 means random port)",
      short_name = "partition_port", default_value = "0")
  public final class PartitionTransportServerPort implements Name<Integer> {
  }

  /**
   * The maximum length which the pending connection queue of
   * {@link edu.snu.vortex.runtime.executor.data.partitiontransfer.PartitionTransport} may grow to.
   */
  @NamedParameter(doc = "The maximum number of pending connections to PartitionTransport server",
      short_name = "partition_backlog", default_value = "128")
  public final class PartitionTransportServerBacklog implements Name<Integer> {
  }

  /**
   * The number of listening threads of
   * {@link edu.snu.vortex.runtime.executor.data.partitiontransfer.PartitionTransport} server.
   */
  @NamedParameter(doc = "The number of listening threads of PartitionTransport server",
      short_name = "partition_threads_listening", default_value = "3")
  public final class PartitionTransportServerNumListeningThreads implements Name<Integer> {
  }

  /**
   * The number of {@link edu.snu.vortex.runtime.executor.data.partitiontransfer.PartitionTransport} server threads
   * which work on accepted connections.
   */
  @NamedParameter(doc = "The number of working threads of PartitionTransport server",
      short_name = "partition_threads_working", default_value = "10")
  public final class PartitionTransportServerNumWorkingThreads implements Name<Integer> {
  }

  /**
   * The number of threads of {@link edu.snu.vortex.runtime.executor.data.partitiontransfer.PartitionTransport} client.
   */
  @NamedParameter(doc = "The number of threads of PartitionTransport client",
      short_name = "partition_threads_client", default_value = "10")
  public final class PartitionTransportClientNumThreads implements Name<Integer> {
  }

  public static final RequiredParameter<String> EXECUTOR_ID = new RequiredParameter<>();
  public static final OptionalParameter<Integer> EXECUTOR_CAPACITY = new OptionalParameter<>();
  public static final RequiredParameter<String> JOB_ID = new RequiredParameter<>();

  public static final ConfigurationModule EXECUTOR_CONF = new JobConf()
      .bindNamedParameter(ExecutorId.class, EXECUTOR_ID)
      .bindNamedParameter(ExecutorCapacity.class, EXECUTOR_CAPACITY)
      .bindNamedParameter(JobId.class, JOB_ID)
      .build();

}
