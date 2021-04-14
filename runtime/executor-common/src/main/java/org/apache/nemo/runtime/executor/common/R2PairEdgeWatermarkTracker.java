package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public final class R2PairEdgeWatermarkTracker implements WatermarkTracker {
  private static final Logger LOG = LoggerFactory.getLogger(R2PairEdgeWatermarkTracker.class.getName());

  private long prevWatermark = 0L;

  private final String lambdaPathEdgeId;
  private final String vmPathEdgeId;
  private final int parallelism;

  private final SingleStageWatermarkTracker lambdaWatermarkTracker;
  private final SingleStageWatermarkTracker vmWatermarkTracker;

  private final Map<String, Long> dataFetcherWatermarkMap;

  private final String taskId;

  private boolean vmPathAllStopped = false;
  private boolean lambdaPathAllStopped = false;

  public R2PairEdgeWatermarkTracker(final String vmPathEdgeId,
                                    final String lambdaPathEdgeId,
                                    final String taskId,
                                    final int parallelism) {
    this.lambdaWatermarkTracker = new SingleStageWatermarkTracker(parallelism, taskId);
    this.vmWatermarkTracker = new SingleStageWatermarkTracker(parallelism, taskId);
    this.dataFetcherWatermarkMap = new HashMap<>();
    this.dataFetcherWatermarkMap.put(lambdaPathEdgeId, 0L);
    this.dataFetcherWatermarkMap.put(vmPathEdgeId, 0L);

    this.lambdaPathEdgeId = lambdaPathEdgeId;
    this.vmPathEdgeId = vmPathEdgeId;
    this.parallelism = parallelism;

    this.taskId = taskId;
  }

  // this will be used for R2
  public void stopAndToggleIndex(final int taskIndex,
                                 final String edgeId) {
    final int index = parallelism == 1 ? 0 : taskIndex;
    if (edgeId.equals(lambdaPathEdgeId)) {
      // stop lambda path
      lambdaWatermarkTracker.stopInputPipeWatermark(index);
      lambdaWatermarkTracker.updateAndGetCurrentWatermark()
        .ifPresent(watermark -> {
          dataFetcherWatermarkMap.put(lambdaPathEdgeId, watermark);
        });

      if (!lambdaWatermarkTracker.updateAndGetCurrentWatermark().isPresent()) {
        lambdaPathAllStopped = true;
      }

      vmPathAllStopped = false;
      vmWatermarkTracker.startInputPipeWatermark(index,
        lambdaWatermarkTracker.getWatermark(index));

    } else {
      // stop vm path path
      lambdaWatermarkTracker.startInputPipeWatermark(index,
        vmWatermarkTracker.getWatermark(index));

      vmWatermarkTracker.stopInputPipeWatermark(index);
      vmWatermarkTracker.updateAndGetCurrentWatermark()
        .ifPresent(watermark -> {
          dataFetcherWatermarkMap.put(vmPathEdgeId, watermark);
        });

      if (!vmWatermarkTracker.updateAndGetCurrentWatermark().isPresent()) {
        vmPathAllStopped = true;
      }
      lambdaPathAllStopped = false;
    }

    LOG.info("Stopped watermark index in task {}, " +
      "lambda: {}, vm: {}", taskId, lambdaWatermarkTracker.getStoppedWatermarks(),
      vmWatermarkTracker.getStoppedWatermarks());
  }

  // this will be used for R3
  public void startIndex(final int taskIndex,
                         final String edgeId) {
    final int index = parallelism == 1 ? 0 : taskIndex;

    if (edgeId.equals(lambdaPathEdgeId)) {
      lambdaPathAllStopped = false;
      lambdaWatermarkTracker.startInputPipeWatermark(index,
        vmWatermarkTracker.getWatermark(index));
    } else {
      vmPathAllStopped = false;
      vmWatermarkTracker.startInputPipeWatermark(index,
        lambdaWatermarkTracker.getWatermark(index));
    }
  }

  @Override
  public Optional<Long> trackAndEmitWatermarks(final String edgeId,
                                               final int taskIndex,
                                               final long watermark) {
    if (lambdaPathAllStopped) {
      final Optional<Long> val =
        vmWatermarkTracker.trackAndEmitWatermarks(vmPathEdgeId, taskIndex, watermark);

      if (val.isPresent()) {
        dataFetcherWatermarkMap.put(edgeId, val.get());
      }

      return val;
    } else if (vmPathAllStopped) {
      final Optional<Long> val = lambdaWatermarkTracker
        .trackAndEmitWatermarks(lambdaPathEdgeId, taskIndex, watermark);

      if (val.isPresent()) {
        dataFetcherWatermarkMap.put(edgeId, val.get());
      }

      return val;
    } else {
      final Optional<Long> val;
      if (edgeId.equals(lambdaPathEdgeId)) {
        val = lambdaWatermarkTracker.trackAndEmitWatermarks(lambdaPathEdgeId, taskIndex, watermark);
      } else {
        val = vmWatermarkTracker.trackAndEmitWatermarks(vmPathEdgeId, taskIndex, watermark);
      }

      if (val.isPresent()) {
        // update output watermark!
        final long outputW = dataFetcherWatermarkMap.get(edgeId);
        if (outputW > val.get()) {
          throw new RuntimeException("Output watermark of " + edgeId + " is greater than the emitted watermark " + outputW + ", " + val.get());
        }

        dataFetcherWatermarkMap.put(edgeId, val.get());

      /*
      LOG.info("R2 pair trackAndEmitWatermark task {} edge {} / {} / {} emit watermark {}, pairStopped {} / prev watermark {}" +
          "dataFetcherWatermarkMap {}",
        taskId, edgeId, taskIndex, watermark, outputW, pairStopped, prevWatermark, dataFetcherWatermarkMap);
        */

        final long minWatermark = Collections.min(dataFetcherWatermarkMap.values());
        return Optional.of(minWatermark);
      } else {
        return Optional.empty();
      }
    }
  }
}
