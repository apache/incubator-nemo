package org.apache.nemo.runtime.executor.data;

import org.apache.nemo.runtime.executor.bytetransfer.ByteTransfer;
import org.apache.nemo.runtime.executor.bytetransfer.ByteTransport;
import org.apache.nemo.runtime.executor.bytetransfer.ByteTransportChannelInitializer;
import org.apache.nemo.runtime.executor.common.datatransfer.AckScheduledService;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;

import javax.inject.Inject;

public final class CyclicDependencyHandler {

  private final PipeManagerWorker pipeManagerWorker;
  private final BlockManagerWorker blockManagerWorker;
  private final ByteTransfer byteTransfer;
  private final ByteTransport byteTransport;
  private final AckScheduledService ackScheduledService;
  private final ByteTransportChannelInitializer byteTransportChannelInitializer;

  @Inject
  private CyclicDependencyHandler(final PipeManagerWorker pipeManagerWorker,
                                  final BlockManagerWorker blockManagerWorker,
                                  final ByteTransfer byteTransfer,
                                  final ByteTransport byteTransport,
                                  final AckScheduledService ackScheduledService,
                                  final ByteTransportChannelInitializer byteTransportChannelInitializer) {
    this.pipeManagerWorker = pipeManagerWorker;
    this.blockManagerWorker = blockManagerWorker;
    this.byteTransfer = byteTransfer;
    this.byteTransport = byteTransport;
    this.ackScheduledService = ackScheduledService;
    this.byteTransportChannelInitializer = byteTransportChannelInitializer;

    byteTransportChannelInitializer.initSetup(
      pipeManagerWorker,
      blockManagerWorker,
      byteTransfer,
      byteTransport,
      ackScheduledService);
  }
}
