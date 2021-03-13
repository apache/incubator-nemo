package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.executor.common.ByteTransportChannelInitializer;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;
import org.apache.nemo.runtime.executor.common.ByteTransport;
import org.apache.nemo.runtime.message.MessageEnvironment;
import org.apache.nemo.runtime.message.NemoNameResolver;
import org.apache.nemo.runtime.message.netty.NettyWorkerEnvironment;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

public final class CyclicDependencyHandler {

  private final PipeManagerWorker pipeManagerWorker;
  private final ByteTransportChannelInitializer byteTransportChannelInitializer;

  @Inject
  private CyclicDependencyHandler(final PipeManagerWorker pipeManagerWorker,
                                  final ByteTransport byteTransport,
                                  // final NemoNameResolver nameResolver,
                                  @Parameter(JobConf.ExecutorId.class) final String executorId,
                                  // final MessageEnvironment messageEnvironment,
                                  final ByteTransportChannelInitializer byteTransportChannelInitializer) {
    this.pipeManagerWorker = pipeManagerWorker;
    this.byteTransportChannelInitializer = byteTransportChannelInitializer;

    // ((NettyWorkerEnvironment) messageEnvironment).setNameResolver(nameResolver, executorId);

    byteTransportChannelInitializer.initSetup(
      byteTransport,
      pipeManagerWorker);
  }
}
