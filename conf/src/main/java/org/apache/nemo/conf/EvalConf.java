package org.apache.nemo.conf;

import org.apache.nemo.conf.JobConf;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.CommandLine;

import javax.inject.Inject;

public final class EvalConf {

  @NamedParameter(doc = "enable offloading or not", short_name = "enable_offloading", default_value = "false")
  public final class EnableOffloading implements Name<Boolean> {
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


  @NamedParameter(short_name = "bottleneck_detection_period", default_value = "1000")
  public static final class BottleneckDetectionPeriod implements Name<Long> {
  }

  @NamedParameter(short_name = "bottleneck_detection_consecutive", default_value = "2")
  public static final class BottleneckDetectionConsecutive implements Name<Integer> {
  }

  @NamedParameter(short_name = "bottleneck_detection_threshold", default_value = "0.75")
  public static final class BottleneckDetectionCpuThreshold implements Name<Double> {
  }

  public final boolean enableOffloading;
  public final int poolSize;
  public final int flushBytes;
  public final int flushCount;

  public final long bottleneckDetectionPeriod;
  public final int bottleneckDetectionConsecutive;
  public final double bottleneckDetectionThreshold;


  @Inject
  private EvalConf(@Parameter(EnableOffloading.class) final boolean enableOffloading,
                   @Parameter(LambdaWarmupPool.class) final int poolSize,
                   @Parameter(FlushBytes.class) final int flushBytes,
                   @Parameter(FlushCount.class) final int flushCount,
                   @Parameter(BottleneckDetectionPeriod.class) final long bottleneckDetectionPeriod,
                   @Parameter(BottleneckDetectionConsecutive.class) final int bottleneckDetectionConsecutive,
                   @Parameter(BottleneckDetectionCpuThreshold.class) final double bottleneckDetectionThreshold) {
    this.enableOffloading = enableOffloading;
    this.poolSize = poolSize;
    this.flushBytes = flushBytes;
    this.flushCount = flushCount;
    this.bottleneckDetectionPeriod = bottleneckDetectionPeriod;
    this.bottleneckDetectionConsecutive = bottleneckDetectionConsecutive;
    this.bottleneckDetectionThreshold = bottleneckDetectionThreshold;
  }

  public Configuration getConfiguration() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(EnableOffloading.class, Boolean.toString(enableOffloading));
    jcb.bindNamedParameter(LambdaWarmupPool.class, Integer.toString(poolSize));
    jcb.bindNamedParameter(FlushBytes.class, Integer.toString(flushBytes));
    jcb.bindNamedParameter(FlushCount.class, Integer.toString(flushCount));
    jcb.bindNamedParameter(BottleneckDetectionPeriod.class, Long.toString(bottleneckDetectionPeriod));
    jcb.bindNamedParameter(BottleneckDetectionConsecutive.class, Integer.toString(bottleneckDetectionConsecutive));
    jcb.bindNamedParameter(BottleneckDetectionCpuThreshold.class, Double.toString(bottleneckDetectionThreshold));
    return jcb.build();
  }


  public static void registerCommandLineArgument(final CommandLine cl) {
    cl.registerShortNameOfClass(EnableOffloading.class);
    cl.registerShortNameOfClass(LambdaWarmupPool.class);
    cl.registerShortNameOfClass(FlushBytes.class);
    cl.registerShortNameOfClass(FlushCount.class);
    cl.registerShortNameOfClass(BottleneckDetectionCpuThreshold.class);
    cl.registerShortNameOfClass(BottleneckDetectionConsecutive.class);
    cl.registerShortNameOfClass(BottleneckDetectionPeriod.class);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("----------EvalConf start---------\n");
    sb.append("enableOffloading: "); sb.append(enableOffloading); sb.append("\n");
    sb.append("poolSize: "); sb.append(poolSize); sb.append("\n");
    sb.append("flushBytes: "); sb.append(flushBytes); sb.append("\n");
    sb.append("flushCount: "); sb.append(flushCount); sb.append("\n");
    sb.append("bottleneckDetectionPeriod: "); sb.append(bottleneckDetectionPeriod); sb.append("\n");
    sb.append("bottleneckDectionConsectutive: "); sb.append(bottleneckDetectionConsecutive); sb.append("\n");
    sb.append("bottleneckDetectionThreshold: "); sb.append(bottleneckDetectionThreshold); sb.append("\n");
    sb.append("-----------EvalConf end----------\n");

    return sb.toString();
  }
}
