package org.apache.nemo.runtime.lambdaexecutor.kafka;

import com.sun.management.OperatingSystemMXBean;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.Finishmark;
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.compiler.frontend.beam.source.UnboundedSourceReadable;
import org.apache.nemo.runtime.executor.common.DataFetcher;
import org.apache.nemo.runtime.executor.common.SourceVertexDataFetcher;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingHeartbeatEvent;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class HandleDataFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(HandleDataFetcher.class.getName());

  private final ScheduledExecutorService pollingTrigger = Executors.newSingleThreadScheduledExecutor();
  private final ExecutorService executorService;
  private final List<DataFetcher> fetchers;
  private boolean pollingTime;
  private boolean cpuTimeFlushTime;
  private final int pollingInterval = 400; // ms

  private boolean closed = false;
  private final OffloadingResultCollector resultCollector;

  private int processedCnt = 0;

  private final int id;

  private final UnboundedSource.CheckpointMark startCheckpointMark;

  private final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag;

  private final OperatingSystemMXBean operatingSystemMXBean;
  private final ThreadMXBean threadMXBean;

  private final int taskIndex;


  public HandleDataFetcher(final int id,
                           final int taskIndex,
                           final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag,
                           final List<DataFetcher> fetchers,
                           final OffloadingResultCollector resultCollector,
                           final UnboundedSource.CheckpointMark startCheckpointMark) {
    LOG.info("Handle data fetcher start");
    this.id = id;
    this.taskIndex = taskIndex;
    this.irDag = irDag;
    this.executorService = Executors.newSingleThreadExecutor();
    this.resultCollector = resultCollector;
    this.fetchers = fetchers;
    this.startCheckpointMark = startCheckpointMark;

    this.operatingSystemMXBean =
      (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    this.threadMXBean = ManagementFactory.getThreadMXBean();

    this.pollingTrigger.scheduleAtFixedRate(() -> {
      pollingTime = true;
    }, pollingInterval, pollingInterval, TimeUnit.MILLISECONDS);

    this.pollingTrigger.scheduleAtFixedRate(() -> {
      cpuTimeFlushTime = true;
    }, 1, 1, TimeUnit.SECONDS);
  }

  public void close() throws Exception {
    closed = true;
    executorService.shutdown();
    LOG.info("Await termination..");
    executorService.awaitTermination(10, TimeUnit.SECONDS);
    LOG.info("End of await termination..");
  }

  /**
   * Process an event generated from the dataFetcher.
   * If the event is an instance of Finishmark, we remove the dataFetcher from the current list.
   * @param event event
   * @param dataFetcher current data fetcher
   */
  private void onEventFromDataFetcher(final Object event,
                                      final DataFetcher dataFetcher) {

    if (event instanceof Finishmark) {
      // We've consumed all the data from this data fetcher.
    } else if (event instanceof Watermark) {
      // Watermark
      //LOG.info("Watermark: {}", event);
      processWatermark(dataFetcher.getOutputCollector(), (Watermark) event);
    } else if (event instanceof TimestampAndValue) {
      // Process data element
      processElement(dataFetcher.getOutputCollector(), (TimestampAndValue) event);
    } else {
      throw new RuntimeException("Invalid type of event: " + event);
    }
  }


  /**
   * Process a data element down the DAG dependency.
   */
  private void processElement(final OutputCollector outputCollector,
                              final TimestampAndValue dataElement) {
    processedCnt += 1;
    outputCollector.setInputTimestamp(dataElement.timestamp);
    outputCollector.emit(dataElement.value);
  }

  private void processWatermark(final OutputCollector outputCollector,
                                       final Watermark watermark) {
    processedCnt += 1;
    outputCollector.emitWatermark(watermark);
  }
}
