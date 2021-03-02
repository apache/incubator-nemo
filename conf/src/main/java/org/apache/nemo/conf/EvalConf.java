package org.apache.nemo.conf;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.CommandLine;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;

public final class EvalConf {

  public static final String AWS_REGION = "ap-northeast-2";

  @NamedParameter(doc = "enable offloading or not", short_name = "enable_offloading", default_value = "false")
  public final class EnableOffloading implements Name<Boolean> {
  }

  @NamedParameter(doc = "enable offloading or not", short_name = "enable_offloading_debug", default_value = "false")
  public final class EnableOffloadingDebug implements Name<Boolean> {
  }

  @NamedParameter(doc = "lambda pool size", short_name = "lambda_warmup_pool", default_value = "100")
  public final class LambdaWarmupPool implements Name<Integer> {
  }

  @NamedParameter(doc = "flush byte size", short_name = "flush_bytes", default_value = "1000000")
  public final class FlushBytes implements Name<Integer> {
  }

  @NamedParameter(doc = "flush count", short_name = "flush_count", default_value = "1000")
  public final class FlushCount implements Name<Integer> {
  }

  @NamedParameter(short_name = "autoscaling", default_value = "true")
  public final class Autoscaling implements Name<Boolean> {}

  @NamedParameter(short_name = "randomselection", default_value = "false")
  public final class RandomSelection implements Name<Boolean> {}

  @NamedParameter(doc = "flush period (ms)", short_name = "flush_period", default_value = "1000")
  public final class FlushPeriod implements Name<Integer> {
  }

  @NamedParameter(doc = "path for sampling json path", short_name = "sampling_path", default_value = "")
  public final class SamplingPath implements Name<String> {
  }

  @NamedParameter(default_value = "")
  public final class SamplingJsonString implements Name<String> {
  }

  // local, lambda, vm
  @NamedParameter(short_name = "offloading_type", default_value = "local")
  public final class OffloadingType implements Name<String> {
  }

  // local, lambda, vm
  @NamedParameter(short_name = "scaling_type", default_value = "migration")
  public final class ScalingType implements Name<String> {
  }

  @NamedParameter(short_name = "scaling_alpha", default_value = "0.6")
  public static final class ScalingAlpha implements Name<Double> {
  }

  @NamedParameter(short_name = "bursty_op", default_value = "")
  public final class BurstyOperatorString implements Name<String> {
  }

  @NamedParameter(short_name = "bottleneck_detection_period", default_value = "1000")
  public static final class BottleneckDetectionPeriod implements Name<Long> {
  }

  @NamedParameter(short_name = "bottleneck_detection_consecutive", default_value = "2")
  public static final class BottleneckDetectionConsecutive implements Name<Integer> {
  }

  @NamedParameter(short_name = "bottleneck_detection_threshold", default_value = "0.9")
  public static final class BottleneckDetectionCpuThreshold implements Name<Double> {
  }

  @NamedParameter(short_name = "deoffloading_threshold", default_value = "0.7")
  public static final class DeoffloadingThreshold implements Name<Double> {
  }

  @NamedParameter(short_name = "event_threshold", default_value = "250000")
  public static final class EventThreshold implements Name<Integer> {
  }

  @NamedParameter(short_name = "min_vm_task", default_value = "8")
  public static final class MinVmTask implements Name<Integer> {
  }

  @NamedParameter(short_name = "is_local_source", default_value = "true")
  public static final class IsLocalSource implements Name<Boolean> {
  }

  @NamedParameter(short_name = "source_parallelism", default_value = "4")
  public static final class SourceParallelism implements Name<Integer> {
  }

  @NamedParameter(short_name = "middle_parallelism", default_value = "1")
  public static final class MiddleParallelism implements Name<Integer> {
  }

  @NamedParameter(short_name = "ec2", default_value = "false")
  public static final class Ec2 implements Name<Boolean> {}

  @NamedParameter(short_name = "executor_threads", default_value = "1")
  public static final class ExecutorThreadNum implements Name<Integer> {}

  @NamedParameter(short_name = "off_executor_threads", default_value = "1")
  public static final class OffExecutorThreadNum implements Name<Integer> {}

  @NamedParameter(short_name = "task_slot", default_value = "2")
  public static final class TaskSlot implements Name<Integer> {}

  @NamedParameter(short_name = "control_logging", default_value = "false")
  public static final class ControlLogging implements Name<Boolean> {}

  @NamedParameter(short_name = "sf_to_vm", default_value = "false")
  public static final class SftoVm implements Name<Boolean> {}

  @NamedParameter(short_name = "aws_region", default_value = "ap-northeast-2")
  public static final class AWSRegion implements Name<String> {}

  // Throttling vm worker for testing !!
  @NamedParameter(short_name = "throttle_rate", default_value = "10000000")
  public static final class ThrottleRate implements Name<Long> {}

  // 1.0: 100 %, if available core = 4, allocated_cores = 1.0, and cpu-limit = 0.5,
  // actual cores = 4 * 1.0 * 0.5
  @NamedParameter(short_name = "allocated_cores", default_value = "1.0")
  public static final class AllocatedCores implements Name<Double> {}

  // 1.0: 100%
  @NamedParameter(short_name = "cpu_limit", default_value = "1.0")
  public static final class CpuLimit implements Name<Double> {}

  @NamedParameter(short_name = "offloading_cpu_limit", default_value = "1.0")
  public static final class OffloadingCpuLimit implements Name<Double> {}

  // # offload workers
  @NamedParameter(short_name = "num_offloading_worker", default_value = "1")
  public static final class NumOffloadingWorker implements Name<Integer> {}

  // # offload workers
  @NamedParameter(short_name = "num_offloading_worker_after_merging", default_value = "1")
  public static final class NumOffloadingWorkerAfterMerging implements Name<Integer> {}

  @NamedParameter(short_name = "destroy_offloading_worker", default_value = "false")
  public static final class DestroyOffloadingWorker implements Name<Boolean> {}

  @NamedParameter(short_name = "offloading_manager", default_value = "shared")
  public static final class OffloadingManagerType implements Name<String> {}

  @NamedParameter(short_name = "aws_profile", default_value = "default")
  public static final class AWSProfileName implements Name<String> {}

  // per executor
  @NamedParameter(short_name = "num_lambda_pool", default_value = "1")
  public static final class NumLambdaPool implements Name<Integer> {}

  public final boolean enableOffloading;
  public final boolean offloadingdebug;
  public final int poolSize;
  public final int flushBytes;
  public final int flushCount;
  public final int flushPeriod;

  public final long bottleneckDetectionPeriod;
  public final int bottleneckDetectionConsecutive;
  public final double bottleneckDetectionThreshold;
  public final String samplingJsonStr;
  public final Map<String, Double> samplingJson;
  public final String burstyOperatorStr;
  public final boolean isLocalSource;
  public final int sourceParallelism;
  public final int minVmTask;
  public final int eventThreshold;
  public final boolean ec2;
  public final double deoffloadingThreshold;
  public final String offloadingType;
  public final int middleParallelism;
  public final int executorThreadNum;
  public final int offExecutorThreadNum;
  public final int taskSlot;
  public final boolean controlLogging;
  public final boolean autoscaling;
  public final boolean randomSelection;
  public final double scalingAlpha;
  public final boolean sfToVm;
  public final String awsRegion;
  public final long throttleRate;

  public final double allocatedCores; // (for testing)
  public final double cpuLimit; // cpu limit for executor  (for testing)
  public final double offloadingCpuLimit; // cpu limit for offloading container (for testing)
  public final int numOffloadingWorker;
  public final int numOffloadingWorkerAfterMerging;
  public final boolean destroyOffloadingWorker;
  public final String scalingType;
  public final String offloadingManagerType;
  public final String awsProfileName;
  public final int numLambdaPool;

  @Inject
  private EvalConf(@Parameter(EnableOffloading.class) final boolean enableOffloading,
                   @Parameter(LambdaWarmupPool.class) final int poolSize,
                   @Parameter(FlushBytes.class) final int flushBytes,
                   @Parameter(FlushCount.class) final int flushCount,
                   @Parameter(FlushPeriod.class) final int flushPeriod,
                   @Parameter(EnableOffloadingDebug.class) final boolean offloadingdebug,
                   @Parameter(Ec2.class) final boolean ec2,
                   @Parameter(BottleneckDetectionPeriod.class) final long bottleneckDetectionPeriod,
                   @Parameter(BottleneckDetectionConsecutive.class) final int bottleneckDetectionConsecutive,
                   @Parameter(BottleneckDetectionCpuThreshold.class) final double bottleneckDetectionThreshold,
                   @Parameter(SamplingJsonString.class) final String samplingJsonStr,
                   @Parameter(BurstyOperatorString.class) final String burstyOperatorStr,
                   @Parameter(IsLocalSource.class) final boolean isLocalSource,
                   @Parameter(SourceParallelism.class) final int sourceParallelism,
                   @Parameter(MinVmTask.class) final int minVmTask,
                   @Parameter(DeoffloadingThreshold.class) final double deoffloadingThreshold,
                   @Parameter(EventThreshold.class) final int eventThreshold,
                   @Parameter(OffloadingType.class) final String offloadingType,
                   @Parameter(MiddleParallelism.class) final int middleParallelism,
                   @Parameter(ExecutorThreadNum.class) final int executorThreadNum,
                   @Parameter(OffExecutorThreadNum.class) final int offExecutorThreadNum,
                   @Parameter(TaskSlot.class) final int taskSlot,
                   @Parameter(ControlLogging.class) final boolean controlLogging,
                   @Parameter(Autoscaling.class) final boolean autoscaling,
                   @Parameter(RandomSelection.class) final boolean randomSelection,
                   @Parameter(ScalingAlpha.class) final double scalingAlpha,
                   @Parameter(SftoVm.class) final boolean sfToVm,
                   @Parameter(ThrottleRate.class) final long throttleRate,
                   @Parameter(AWSRegion.class) final String awsRegion,
                   @Parameter(AllocatedCores.class) final double allocatedCores,
                   @Parameter(CpuLimit.class) final double cpuLimit,
                   @Parameter(ScalingType.class) final String scalingType,
                   @Parameter(OffloadingCpuLimit.class) final double offloadingCpuLimit,
                   @Parameter(NumOffloadingWorker.class) final int numOffloadingWorker,
                   @Parameter(NumOffloadingWorkerAfterMerging.class) final int numOffloadingWorkerAfterMerging,
                   @Parameter(OffloadingManagerType.class) final String offloadingManagerType,
                   @Parameter(AWSProfileName.class) final String awsProfileName,
                   @Parameter(DestroyOffloadingWorker.class) final boolean destroyOffloadingWorker,
                   @Parameter(NumLambdaPool.class) final int numLambdaPool) throws IOException {
    this.enableOffloading = enableOffloading;
    this.offloadingdebug = offloadingdebug;
    this.poolSize = poolSize;
    this.flushBytes = flushBytes;
    this.scalingType = scalingType;
    this.offExecutorThreadNum = offExecutorThreadNum;
    this.deoffloadingThreshold = deoffloadingThreshold;
    this.ec2 = ec2;
    this.flushCount = flushCount;
    this.flushPeriod = flushPeriod;
    this.throttleRate = throttleRate;
    this.bottleneckDetectionPeriod = bottleneckDetectionPeriod;
    this.bottleneckDetectionConsecutive = bottleneckDetectionConsecutive;
    this.bottleneckDetectionThreshold = bottleneckDetectionThreshold;
    this.samplingJsonStr = samplingJsonStr;
    this.burstyOperatorStr = burstyOperatorStr;
    this.isLocalSource = isLocalSource;
    this.sourceParallelism = sourceParallelism;
    this.minVmTask = minVmTask;
    this.eventThreshold = eventThreshold;
    this.offloadingType = offloadingType;
    this.middleParallelism = middleParallelism;
    this.executorThreadNum = executorThreadNum;
    this.taskSlot = taskSlot;
    this.controlLogging = controlLogging;
    this.autoscaling = autoscaling;
    this.randomSelection = randomSelection;
    this.scalingAlpha = scalingAlpha;
    this.sfToVm = sfToVm;
    this.awsRegion = awsRegion;
    this.allocatedCores = allocatedCores;
    this.cpuLimit = cpuLimit;
    this.offloadingCpuLimit = offloadingCpuLimit;
    this.numOffloadingWorker = numOffloadingWorker;
    this.destroyOffloadingWorker = destroyOffloadingWorker;
    this.offloadingManagerType = offloadingManagerType;
    this.numOffloadingWorkerAfterMerging = numOffloadingWorkerAfterMerging;
    this.awsProfileName = awsProfileName;
    this.numLambdaPool = numLambdaPool;

    if (!samplingJsonStr.isEmpty()) {
      this.samplingJson = new ObjectMapper().readValue(samplingJsonStr, new TypeReference<Map<String, Double>>(){});
    } else {
      this.samplingJson = new HashMap<>();
    }
    System.out.println("Sampling json: " + samplingJson);
  }

  public Configuration getConfiguration() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(EnableOffloading.class, Boolean.toString(enableOffloading));
    jcb.bindNamedParameter(EnableOffloadingDebug.class, Boolean.toString(offloadingdebug));
    jcb.bindNamedParameter(LambdaWarmupPool.class, Integer.toString(poolSize));
    jcb.bindNamedParameter(FlushBytes.class, Integer.toString(flushBytes));
    jcb.bindNamedParameter(FlushCount.class, Integer.toString(flushCount));
    jcb.bindNamedParameter(FlushPeriod.class, Integer.toString(flushPeriod));
    jcb.bindNamedParameter(BottleneckDetectionPeriod.class, Long.toString(bottleneckDetectionPeriod));
    jcb.bindNamedParameter(BottleneckDetectionConsecutive.class, Integer.toString(bottleneckDetectionConsecutive));
    jcb.bindNamedParameter(BottleneckDetectionCpuThreshold.class, Double.toString(bottleneckDetectionThreshold));
    jcb.bindNamedParameter(SamplingJsonString.class, samplingJsonStr);
    jcb.bindNamedParameter(BurstyOperatorString.class, burstyOperatorStr);
    jcb.bindNamedParameter(IsLocalSource.class, Boolean.toString(isLocalSource));
    jcb.bindNamedParameter(SourceParallelism.class, Integer.toString(sourceParallelism));
    jcb.bindNamedParameter(MinVmTask.class, Integer.toString(minVmTask));
    jcb.bindNamedParameter(EventThreshold.class, Integer.toString(eventThreshold));
    jcb.bindNamedParameter(Ec2.class, Boolean.toString(ec2));
    jcb.bindNamedParameter(DeoffloadingThreshold.class, Double.toString(deoffloadingThreshold));
    jcb.bindNamedParameter(OffloadingType.class, offloadingType);
    jcb.bindNamedParameter(MiddleParallelism.class, Integer.toString(middleParallelism));
    jcb.bindNamedParameter(ExecutorThreadNum.class, Integer.toString(executorThreadNum));
    jcb.bindNamedParameter(TaskSlot.class, Integer.toString(taskSlot));
    jcb.bindNamedParameter(OffExecutorThreadNum.class, Integer.toString(offExecutorThreadNum));
    jcb.bindNamedParameter(ControlLogging.class, Boolean.toString(controlLogging));
    jcb.bindNamedParameter(Autoscaling.class, Boolean.toString(autoscaling));
    jcb.bindNamedParameter(RandomSelection.class, Boolean.toString(randomSelection));
    jcb.bindNamedParameter(ScalingAlpha.class, Double.toString(scalingAlpha));
    jcb.bindNamedParameter(SftoVm.class, Boolean.toString(sfToVm));
    jcb.bindNamedParameter(AWSRegion.class, awsRegion);
    jcb.bindNamedParameter(ThrottleRate.class, Long.toString(throttleRate));
    jcb.bindNamedParameter(AllocatedCores.class, Double.toString(allocatedCores));
    jcb.bindNamedParameter(CpuLimit.class, Double.toString(cpuLimit));
    jcb.bindNamedParameter(OffloadingCpuLimit.class, Double.toString(offloadingCpuLimit));
    jcb.bindNamedParameter(NumOffloadingWorker.class, Integer.toString(numOffloadingWorker));
    jcb.bindNamedParameter(NumOffloadingWorkerAfterMerging.class, Integer.toString(numOffloadingWorkerAfterMerging));
    jcb.bindNamedParameter(DestroyOffloadingWorker.class, Boolean.toString(destroyOffloadingWorker));
    jcb.bindNamedParameter(ScalingType.class, scalingType);
    jcb.bindNamedParameter(OffloadingManagerType.class, offloadingManagerType);
    jcb.bindNamedParameter(AWSProfileName.class, awsProfileName);
    jcb.bindNamedParameter(NumLambdaPool.class, Integer.toString(numLambdaPool));
    return jcb.build();
  }


  public static void registerCommandLineArgument(final CommandLine cl) {
    cl.registerShortNameOfClass(EnableOffloading.class);
    cl.registerShortNameOfClass(EnableOffloadingDebug.class);
    cl.registerShortNameOfClass(LambdaWarmupPool.class);
    cl.registerShortNameOfClass(FlushBytes.class);
    cl.registerShortNameOfClass(FlushCount.class);
    cl.registerShortNameOfClass(FlushPeriod.class);
    cl.registerShortNameOfClass(BottleneckDetectionCpuThreshold.class);
    cl.registerShortNameOfClass(BottleneckDetectionConsecutive.class);
    cl.registerShortNameOfClass(BottleneckDetectionPeriod.class);
    cl.registerShortNameOfClass(SamplingPath.class);
    cl.registerShortNameOfClass(BurstyOperatorString.class);
    cl.registerShortNameOfClass(IsLocalSource.class);
    cl.registerShortNameOfClass(SourceParallelism.class);
    cl.registerShortNameOfClass(MinVmTask.class);
    cl.registerShortNameOfClass(EventThreshold.class);
    cl.registerShortNameOfClass(Ec2.class);
    cl.registerShortNameOfClass(DeoffloadingThreshold.class);
    cl.registerShortNameOfClass(OffloadingType.class);
    cl.registerShortNameOfClass(MiddleParallelism.class);
    cl.registerShortNameOfClass(ExecutorThreadNum.class);
    cl.registerShortNameOfClass(TaskSlot.class);
    cl.registerShortNameOfClass(OffExecutorThreadNum.class);
    cl.registerShortNameOfClass(ControlLogging.class);
    cl.registerShortNameOfClass(Autoscaling.class);
    cl.registerShortNameOfClass(RandomSelection.class);
    cl.registerShortNameOfClass(ScalingAlpha.class);
    cl.registerShortNameOfClass(SftoVm.class);
    cl.registerShortNameOfClass(AWSRegion.class);
    cl.registerShortNameOfClass(ThrottleRate.class);
    cl.registerShortNameOfClass(AllocatedCores.class);
    cl.registerShortNameOfClass(CpuLimit.class);
    cl.registerShortNameOfClass(OffloadingCpuLimit.class);
    cl.registerShortNameOfClass(NumOffloadingWorker.class);
    cl.registerShortNameOfClass(NumOffloadingWorkerAfterMerging.class);
    cl.registerShortNameOfClass(DestroyOffloadingWorker.class);
    cl.registerShortNameOfClass(ScalingType.class);
    cl.registerShortNameOfClass(OffloadingManagerType.class);
    cl.registerShortNameOfClass(AWSProfileName.class);
    cl.registerShortNameOfClass(NumLambdaPool.class);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("----------EvalConf start---------\n");
    sb.append("enableOffloading: "); sb.append(enableOffloading); sb.append("\n");
    sb.append("enableOffloadingDebug: "); sb.append(offloadingdebug); sb.append("\n");
    sb.append("ec2: "); sb.append(ec2); sb.append("\n");
    sb.append("poolSize: "); sb.append(poolSize); sb.append("\n");
    sb.append("flushBytes: "); sb.append(flushBytes); sb.append("\n");
    sb.append("flushCount: "); sb.append(flushCount); sb.append("\n");
    sb.append("flushPeriod: "); sb.append(flushPeriod); sb.append("\n");
    sb.append("sampling: "); sb.append(samplingJsonStr); sb.append("\n");
    sb.append("bottleneckDetectionPeriod: "); sb.append(bottleneckDetectionPeriod); sb.append("\n");
    sb.append("bottleneckDectionConsectutive: "); sb.append(bottleneckDetectionConsecutive); sb.append("\n");
    sb.append("bottleneckDetectionThreshold: "); sb.append(bottleneckDetectionThreshold); sb.append("\n");
    sb.append("deoffloadingThreshold: "); sb.append(deoffloadingThreshold); sb.append("\n");
    sb.append("samplingJson: "); sb.append(samplingJsonStr); sb.append("\n");
    sb.append("burstyOps: "); sb.append(burstyOperatorStr); sb.append("\n");
    sb.append("isLocalSource: "); sb.append(isLocalSource); sb.append("\n");
    sb.append("sourceParallelism: "); sb.append(sourceParallelism); sb.append("\n");
    sb.append("minVmTask: "); sb.append(minVmTask); sb.append("\n");
    sb.append("eventThreshold: "); sb.append(eventThreshold); sb.append("\n");
    sb.append("offloadingType: "); sb.append(offloadingType); sb.append("\n");
    sb.append("middleParallelism: "); sb.append(middleParallelism); sb.append("\n");
    sb.append("executorThreadNum: "); sb.append(executorThreadNum); sb.append("\n");
    sb.append("taskSlotNum: "); sb.append(taskSlot); sb.append("\n");
    sb.append("offExecutorThreadNum: "); sb.append(offExecutorThreadNum); sb.append("\n");
    sb.append("controlLogging: "); sb.append(controlLogging); sb.append("\n");
    sb.append("autoscaling: "); sb.append(autoscaling); sb.append("\n");
    sb.append("randomselection: "); sb.append(randomSelection); sb.append("\n");
    sb.append("awsRegion: "); sb.append(awsRegion); sb.append("\n");
    sb.append("throttleRate: "); sb.append(throttleRate); sb.append("\n");
    sb.append("allocatedCores: "); sb.append(allocatedCores); sb.append("\n");
    sb.append("cpuLimit: "); sb.append(cpuLimit); sb.append("\n");
    sb.append("offloadingCpuLimit: "); sb.append(offloadingCpuLimit); sb.append("\n");
    sb.append("numOffloadingWorker: "); sb.append(numOffloadingWorker); sb.append("\n");
    sb.append("numOffloadingWorkerAfterMerging: "); sb.append(numOffloadingWorkerAfterMerging); sb.append("\n");
    sb.append("destroyOffloadingWorker: "); sb.append(destroyOffloadingWorker); sb.append("\n");
    sb.append("scalingType: "); sb.append(scalingType); sb.append("\n");
    sb.append("offloadingManagerType: "); sb.append(offloadingManagerType); sb.append("\n");
    sb.append("awsProfileName: "); sb.append(awsProfileName); sb.append("\n");
    sb.append("numLambdaPool: "); sb.append(numLambdaPool); sb.append("\n");
    sb.append("-----------EvalConf end----------\n");

    return sb.toString();
  }
}
