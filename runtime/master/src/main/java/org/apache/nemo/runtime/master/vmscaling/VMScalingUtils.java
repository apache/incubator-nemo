package org.apache.nemo.runtime.master.vmscaling;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.common.OffloadingMasterEvent;
import org.apache.nemo.runtime.master.VMScalingWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public final class VMScalingUtils {
  private static final Logger LOG = LoggerFactory.getLogger(VMScalingUtils.class.getName());
  private final AmazonEC2 ec2;
  private final AtomicInteger numVMs = new AtomicInteger(0);

  private final List<String> vmAddresses;
  private final List<String> instanceIds;

  @Inject
  private VMScalingUtils(final EvalConf evalConf,
                         final VMScalingAddresses vmScalingAddresses) {
    this.ec2 = AmazonEC2ClientBuilder.standard()
      .withRegion(evalConf.awsRegion).build();
    this.vmAddresses = vmScalingAddresses.vmAddresses;
    this.instanceIds = vmScalingAddresses.vmIds;
  }

  private final List<String> startedInstances = new LinkedList<>();

  public synchronized void startInstances(int num) {
     final int currNum = numVMs.get();
    final List<String> addresses = vmAddresses.subList(currNum, currNum + num);
    final List<String> ids = instanceIds.subList(currNum, currNum + num);
    numVMs.addAndGet(num);

    startedInstances.addAll(ids);

    final StartInstancesRequest startRequest = new StartInstancesRequest()
      .withInstanceIds(ids);
    ec2.startInstances(startRequest);
    LOG.info("Starting ec2 instances {}/{}", ids, System.currentTimeMillis());
  }

  public synchronized void stopVMs(final int num) {
    stopVM(startedInstances);
    startedInstances.clear();
  }

  private void stopVM(final List<String> stopIds) {
    //  final DescribeInstancesRequest request = new DescribeInstancesRequest();
    // request.setInstanceIds(stopIds);
    // final DescribeInstancesResult response = ec2.describeInstances(request);
    final StopInstancesRequest stopRequest = new StopInstancesRequest()
      .withInstanceIds(stopIds);
    LOG.info("Stopping ec2 instances {}/{}", stopIds, System.currentTimeMillis());
    ec2.stopInstances(stopRequest);
  }
}
