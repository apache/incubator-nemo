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

  @NamedParameter(short_name = "bp_queue_lower_bound", default_value = "5000")
  public static final class BPQueueLowerBound implements Name<Long> {}

  @NamedParameter(short_name = "bp_increase_ratio", default_value = "2.0")
  public static final class BPIncreaseRatio implements Name<Double> {}

  @NamedParameter(short_name = "bp_decrease_ratio", default_value = "0.7")
  public static final class BPDecreaseRatio implements Name<Double> {}

  @NamedParameter(short_name = "bp_increase_lower_cpu", default_value = "0.7")
  public static final class BPIncraseLowerCpu implements Name<Double> {}

  public final long bpQueueUpperBound;
  public final long bpQueueLowerBound;
  public final double bpIncreaseRatio;
  public final double bpDecreaseRatio;
  public final double bpIncreaseLowerCpu;
  // End of backpressure

  // Scaling policy parameters


  @Inject
  private PolicyConf(@Parameter(BPQueueUpperBound.class) final long bpQueueSize,
                     @Parameter(BPQueueLowerBound.class) final long bpQueueLowerBound,
                     @Parameter(BPIncreaseRatio.class) final double bpIncreaseRatio,
                     @Parameter(BPDecreaseRatio.class) final double bpDecreaseRatio,
                     @Parameter(BPIncraseLowerCpu.class) final double bpIncreaseLowerCpu) throws IOException {
    this.bpQueueUpperBound = bpQueueSize;
    this.bpQueueLowerBound = bpQueueLowerBound;
    this.bpIncreaseRatio = bpIncreaseRatio;
    this.bpDecreaseRatio = bpDecreaseRatio;
    this.bpIncreaseLowerCpu = bpIncreaseLowerCpu;
  }

  public Configuration getConfiguration() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(BPQueueUpperBound.class, Long.toString(bpQueueUpperBound));
    jcb.bindNamedParameter(BPQueueLowerBound.class, Long.toString(bpQueueLowerBound));
    jcb.bindNamedParameter(BPIncreaseRatio.class, Double.toString(bpIncreaseRatio));
    jcb.bindNamedParameter(BPDecreaseRatio.class, Double.toString(bpDecreaseRatio));
    jcb.bindNamedParameter(BPIncraseLowerCpu.class, Double.toString(bpIncreaseLowerCpu));
    return jcb.build();
  }


  public static void registerCommandLineArgument(final CommandLine cl) {
    cl.registerShortNameOfClass(BPQueueUpperBound.class);
    cl.registerShortNameOfClass(BPQueueLowerBound.class);
    cl.registerShortNameOfClass(BPIncreaseRatio.class);
    cl.registerShortNameOfClass(BPDecreaseRatio.class);
    cl.registerShortNameOfClass(BPIncraseLowerCpu.class);
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
    sb.append("-----------PolicyConf end----------\n");

    return sb.toString();
  }
}
