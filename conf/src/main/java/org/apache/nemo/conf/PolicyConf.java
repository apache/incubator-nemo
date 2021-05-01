package org.apache.nemo.conf;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.CommandLine;

import javax.inject.Inject;
import java.io.IOException;

public final class PolicyConf {

  // Backpressure parameters
  @NamedParameter(short_name = "bp_queue_upper_bound", default_value = "20000")
  public static final class BPQueueUpperBound implements Name<Long> {}

  @NamedParameter(short_name = "bp_queue_lower_bound", default_value = "10000")
  public static final class BPQueueLowerBound implements Name<Long> {}

  @NamedParameter(short_name = "bp_increase_ratio", default_value = "1.5")
  public static final class BPIncreaseRatio implements Name<Double> {}

  @NamedParameter(short_name = "bp_decrease_ratio", default_value = "0.8")
  public static final class BPDecreaseRatio implements Name<Double> {}

  @NamedParameter(short_name = "bp_increase_lower_cpu", default_value = "0.8")
  public static final class BPIncraseLowerCpu implements Name<Double> {}

  @NamedParameter(short_name = "bp_min_event", default_value = "5000")
  public static final class BPMinEvent implements Name<Long> {}

  public final long bpQueueUpperBound;
  public final long bpQueueLowerBound;
  public final double bpIncreaseRatio;
  public final double bpDecreaseRatio;
  public final double bpIncreaseLowerCpu;
  public final long bpMinEvent;
  // End of backpressure

  // Scaling policy parameters
  @NamedParameter(short_name = "scaler_upper_cpu", default_value = "1.2")
  public static final class ScalerUpperCPU implements Name<Double> {}

  @NamedParameter(short_name = "scaler_target_cpu", default_value = "0.6")
  public static final class ScalerTargetScaleoutCPU implements Name<Double> {}

  @NamedParameter(short_name = "scaler_scaleout_trigger_cpu", default_value = "0.8")
  public static final class ScalerScaleoutTriggerCPU implements Name<Double> {}

  @NamedParameter(short_name = "scaler_scalein_trigger_cpu", default_value = "0.5")
  public static final class ScalerScaleInTriggerCPU implements Name<Double> {}

  @NamedParameter(short_name = "scaler_trigger_window", default_value = "3")
  public static final class ScalerTriggerWindow implements Name<Integer> {}

  // sec
  @NamedParameter(short_name = "scaler_slack_time", default_value = "5")
  public static final class ScalerSlackTime implements Name<Integer> {}

  public final double scalerUpperCpu;
  public final double scalerTargetCpu;
  public final double scalerScaleoutTriggerCPU;
  public final int scalerTriggerWindow;
  public final int scalerSlackTime;


  @Inject
  private PolicyConf(@Parameter(BPQueueUpperBound.class) final long bpQueueSize,
                     @Parameter(BPQueueLowerBound.class) final long bpQueueLowerBound,
                     @Parameter(BPIncreaseRatio.class) final double bpIncreaseRatio,
                     @Parameter(BPDecreaseRatio.class) final double bpDecreaseRatio,
                     @Parameter(BPIncraseLowerCpu.class) final double bpIncreaseLowerCpu,
                     @Parameter(BPMinEvent.class) final long bpMinEvent,
                     @Parameter(ScalerUpperCPU.class) final double scalerUpperCpu,
                     @Parameter(ScalerTargetScaleoutCPU.class) final double scalerTargetCpu,
                     @Parameter(ScalerScaleoutTriggerCPU.class) final double scalerScaleoutTriggerCpu,
                     @Parameter(ScalerTriggerWindow.class) final int scalerTriggerWindow,
                     @Parameter(ScalerSlackTime.class) final int scalerSlackTime) throws IOException {
    this.bpQueueUpperBound = bpQueueSize;
    this.bpQueueLowerBound = bpQueueLowerBound;
    this.bpIncreaseRatio = bpIncreaseRatio;
    this.bpDecreaseRatio = bpDecreaseRatio;
    this.bpIncreaseLowerCpu = bpIncreaseLowerCpu;
    this.bpMinEvent = bpMinEvent;

    this.scalerUpperCpu = scalerUpperCpu;
    this.scalerTargetCpu = scalerTargetCpu;
    this.scalerScaleoutTriggerCPU = scalerScaleoutTriggerCpu;
    this.scalerTriggerWindow = scalerTriggerWindow;
    this.scalerSlackTime = scalerSlackTime;
  }

  public Configuration getConfiguration() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(BPQueueUpperBound.class, Long.toString(bpQueueUpperBound));
    jcb.bindNamedParameter(BPQueueLowerBound.class, Long.toString(bpQueueLowerBound));
    jcb.bindNamedParameter(BPIncreaseRatio.class, Double.toString(bpIncreaseRatio));
    jcb.bindNamedParameter(BPDecreaseRatio.class, Double.toString(bpDecreaseRatio));
    jcb.bindNamedParameter(BPIncraseLowerCpu.class, Double.toString(bpIncreaseLowerCpu));
    jcb.bindNamedParameter(BPMinEvent.class, Double.toString(bpMinEvent));

    jcb.bindNamedParameter(ScalerUpperCPU.class, Double.toString(scalerUpperCpu));
    jcb.bindNamedParameter(ScalerTargetScaleoutCPU.class, Double.toString(scalerTargetCpu));
    jcb.bindNamedParameter(ScalerTriggerWindow.class, Integer.toString(scalerTriggerWindow));
    jcb.bindNamedParameter(ScalerSlackTime.class, Integer.toString(scalerSlackTime));
    jcb.bindNamedParameter(ScalerScaleoutTriggerCPU.class, Double.toString(scalerScaleoutTriggerCPU));
    return jcb.build();
  }


  public static void registerCommandLineArgument(final CommandLine cl) {
    cl.registerShortNameOfClass(BPQueueUpperBound.class);
    cl.registerShortNameOfClass(BPQueueLowerBound.class);
    cl.registerShortNameOfClass(BPIncreaseRatio.class);
    cl.registerShortNameOfClass(BPDecreaseRatio.class);
    cl.registerShortNameOfClass(BPIncraseLowerCpu.class);
    cl.registerShortNameOfClass(BPMinEvent.class);

    cl.registerShortNameOfClass(ScalerUpperCPU.class);
    cl.registerShortNameOfClass(ScalerTargetScaleoutCPU.class);
    cl.registerShortNameOfClass(ScalerTriggerWindow.class);
    cl.registerShortNameOfClass(ScalerSlackTime.class);
    cl.registerShortNameOfClass(ScalerScaleoutTriggerCPU.class);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("---------PolicyConf- start---------\n");
    sb.append("bpQueueUpperBound: "); sb.append(bpQueueUpperBound); sb.append("\n");
    sb.append("bpQueueLowerBound: "); sb.append(bpQueueLowerBound); sb.append("\n");
    sb.append("bpIncreaseRatio: "); sb.append(bpIncreaseRatio); sb.append("\n");
    sb.append("bpDecreaseRatio: "); sb.append(bpDecreaseRatio); sb.append("\n");
    sb.append("bpIncreaseLowerCPu: "); sb.append(bpIncreaseLowerCpu); sb.append("\n");
    sb.append("bpMinEvent: "); sb.append(bpMinEvent); sb.append("\n");

    sb.append("scalerUpperCPU: "); sb.append(scalerUpperCpu); sb.append("\n");
    sb.append("scalerTargerCPU: "); sb.append(scalerTargetCpu); sb.append("\n");
    sb.append("scalerScaleoutTriggerCPU: "); sb.append(scalerScaleoutTriggerCPU); sb.append("\n");
    sb.append("scalerTriggerWindow: "); sb.append(scalerTriggerWindow); sb.append("\n");
    sb.append("scalerSlackTime: "); sb.append(scalerSlackTime); sb.append("\n");
    sb.append("-----------PolicyConf end----------\n");

    return sb.toString();
  }
}
