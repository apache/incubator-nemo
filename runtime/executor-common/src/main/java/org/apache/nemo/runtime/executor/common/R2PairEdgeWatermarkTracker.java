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

  private final R2SingleStageWatermarkTracker lambdaWatermarkTracker;
  private final R2SingleStageWatermarkTracker vmWatermarkTracker;

  private final Map<String, Long> dataFetcherWatermarkMap;

  private final String taskId;

  private boolean vmPathAllStopped = false;
  private boolean lambdaPathAllStopped = false;

  public R2PairEdgeWatermarkTracker(final String vmPathEdgeId,
                                    final String lambdaPathEdgeId,
                                    final String taskId,
                                    final int parallelism) {
    this.lambdaWatermarkTracker = new R2SingleStageWatermarkTracker(parallelism);
    this.vmWatermarkTracker = new R2SingleStageWatermarkTracker(parallelism);
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
      LOG.info("Stop lambda path watermark index in task {} / {}, " +
          "lambda: {}, vm: {}", taskId, taskIndex, lambdaWatermarkTracker.getStoppedWatermarks(),
        vmWatermarkTracker.getStoppedWatermarks());
      lambdaWatermarkTracker.stopInputPipeWatermark(index);

      lambdaWatermarkTracker.updateAndGetCurrentWatermark()
        .ifPresent(watermark -> {
          dataFetcherWatermarkMap.put(lambdaPathEdgeId, watermark);
        });

      vmPathAllStopped = false;
      vmWatermarkTracker.startInputPipeWatermark(index,
        lambdaWatermarkTracker.getWatermark(index));

      if (!lambdaWatermarkTracker.updateAndGetCurrentWatermark().isPresent()) {
        lambdaPathAllStopped = true;
      }

    } else {
      // stop vm path path
      LOG.info("Stop vm path watermark index in task {} / {}, " +
          "lambda: {}, vm: {}", taskId, taskIndex, lambdaWatermarkTracker.getStoppedWatermarks(),
        vmWatermarkTracker.getStoppedWatermarks());
      vmWatermarkTracker.stopInputPipeWatermark(index);

      vmWatermarkTracker.updateAndGetCurrentWatermark()
        .ifPresent(watermark -> {
          dataFetcherWatermarkMap.put(vmPathEdgeId, watermark);
        });

      lambdaPathAllStopped = false;
      lambdaWatermarkTracker.startInputPipeWatermark(index,
        vmWatermarkTracker.getWatermark(index));

      if (!vmWatermarkTracker.updateAndGetCurrentWatermark().isPresent()) {
        vmPathAllStopped = true;
      }
    }
  }


  // this will be used for R3
  public void startIndexAndStop(final int taskIndex,
                         final String edgeId) {
    final int index = parallelism == 1 ? 0 : taskIndex;
    if (edgeId.equals(lambdaPathEdgeId)) {
      // start lambda path
       // stop vm path path
      LOG.info("Before Stop vm path watermark index in task {}/{}/{}, vm: {}, lambda: {}, map: {}",
        taskId, taskIndex, edgeId, vmWatermarkTracker, lambdaWatermarkTracker, dataFetcherWatermarkMap);
      vmWatermarkTracker.stopInputPipeWatermark(index);

      vmWatermarkTracker.updateAndGetCurrentWatermark()
        .ifPresent(watermark -> {
          dataFetcherWatermarkMap.put(vmPathEdgeId, watermark);
        });

      lambdaPathAllStopped = false;
      lambdaWatermarkTracker.startInputPipeWatermark(index,
        vmWatermarkTracker.getWatermark(index));

      lambdaWatermarkTracker.updateAndGetCurrentWatermark()
        .ifPresent(watermark -> {
          dataFetcherWatermarkMap.put(lambdaPathEdgeId, watermark);
        });

      if (!vmWatermarkTracker.updateAndGetCurrentWatermark().isPresent()) {
        vmPathAllStopped = true;
      }

      LOG.info("After Stop vm path in task " + taskId + "/" + taskIndex + "/" + edgeId
        + "VM tracker: {}, Lambda tracker: {}, map: {}", vmWatermarkTracker, lambdaWatermarkTracker, dataFetcherWatermarkMap);
    } else {
      // start vm path
      // stop lambda path path
        LOG.info("Before Stop lambda path watermark index in task {}/{}/{}, vm: {}, lambda: {}, map: {}"
          , taskId, taskIndex, edgeId, vmWatermarkTracker, lambdaWatermarkTracker, dataFetcherWatermarkMap);

      lambdaWatermarkTracker.stopInputPipeWatermark(index);

      lambdaWatermarkTracker.updateAndGetCurrentWatermark()
        .ifPresent(watermark -> {
          dataFetcherWatermarkMap.put(lambdaPathEdgeId, watermark);
        });

      vmPathAllStopped = false;

      vmWatermarkTracker.startInputPipeWatermark(index,
        lambdaWatermarkTracker.getWatermark(index));

      vmWatermarkTracker.updateAndGetCurrentWatermark()
        .ifPresent(watermark -> {
          dataFetcherWatermarkMap.put(vmPathEdgeId, watermark);
        });

      if (!lambdaWatermarkTracker.updateAndGetCurrentWatermark().isPresent()) {
        lambdaPathAllStopped = true;
      }

      LOG.info("After Stop lambda path in task " + taskId + "/" + taskIndex + "/" + edgeId
        + "VM tracker: {}, Lambda tracker: {}, map: {}", vmWatermarkTracker, lambdaWatermarkTracker, dataFetcherWatermarkMap);
    }
  }

  @Override
  public Optional<Long> trackAndEmitWatermarks(final String taskId,
                                               final String edgeId,
                                               final int taskIndex,
                                               final long watermark) {

    LOG.info("Receive watermark {} / index {} edge {} task {} / " +
        "lambdaStopoped: {} vmStopped: {} prevWatermark: {} dataFetcherWatermarkAMp: {}, " +
        "vmWatermarkTracker: {}, lambdaWatermarkTracker: {}", watermark, taskIndex, edgeId, taskId,
      lambdaPathAllStopped, vmPathAllStopped, prevWatermark, dataFetcherWatermarkMap,
      vmWatermarkTracker, lambdaWatermarkTracker);

    if (lambdaPathAllStopped) {
      if (edgeId.equals(lambdaPathEdgeId)) {
        throw new RuntimeException("Lambda path stopped .. but receive watermark");
      }

      final Optional<Long> val =
        vmWatermarkTracker.trackAndEmitWatermarks(taskId, vmPathEdgeId, taskIndex, watermark);

      if (val.isPresent()) {
        dataFetcherWatermarkMap.put(edgeId, val.get());
        if (val.get() < prevWatermark) {
          return Optional.empty();
        } else {
          prevWatermark = val.get();
        }
      }

      return val;
    } else if (vmPathAllStopped) {
       if (edgeId.equals(vmPathEdgeId)) {
        throw new RuntimeException("VM path stopped .. but receive watermark");
      }

      final Optional<Long> val = lambdaWatermarkTracker
        .trackAndEmitWatermarks(taskId, lambdaPathEdgeId, taskIndex, watermark);

      if (val.isPresent()) {
        dataFetcherWatermarkMap.put(edgeId, val.get());
        if (val.get() < prevWatermark) {
          return Optional.empty();
        } else {
          prevWatermark = val.get();
        }
      }

      return val;
    } else {
      final Optional<Long> val;
      if (edgeId.equals(lambdaPathEdgeId)) {
        val = lambdaWatermarkTracker.trackAndEmitWatermarks(taskId, lambdaPathEdgeId, taskIndex, watermark);
      } else {
        val = vmWatermarkTracker.trackAndEmitWatermarks(taskId, vmPathEdgeId, taskIndex, watermark);
      }

      if (val.isPresent()) {
        // update output watermark!
        final long outputW = dataFetcherWatermarkMap.get(edgeId);
        if (outputW > val.get()) {
          throw new RuntimeException("Output watermark of " + edgeId + " is greater than the emitted watermark " + outputW + ", " + val.get());
        }

        dataFetcherWatermarkMap.put(edgeId, val.get());

      LOG.info("R2 pair trackAndEmitWatermark task {} edge {} / {} / {} emit watermark {} prevW: {} " +
          "dataFetcherWatermarkMap {}",
        taskId, edgeId, taskIndex, watermark, outputW, prevWatermark, dataFetcherWatermarkMap);

        final long minWatermark = Collections.min(dataFetcherWatermarkMap.values());
        if (minWatermark < prevWatermark) {
          return Optional.empty();
        } else {
          return Optional.of(minWatermark);
        }
      } else {
        return Optional.empty();
      }
    }
  }

  @Override
  public String toString() {
    return "LambdaPathAllStopped: " + lambdaPathAllStopped + ", "  +
      "VMPathAllStopped: " + vmPathAllStopped + ", " +
      "DataFetcherWatermarkMap: " + dataFetcherWatermarkMap + ", " +
      "PrevW: " + prevWatermark;
  }
}
