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
  private boolean lambdaPathAllStopped = true;


  private WatermarkTracker watermarkTracker;

  private final WatermarkTracker lambdaPathTracker;
  private final WatermarkTracker vmPathTracker;
  private final WatermarkTracker bothPathTracker;

  public R2PairEdgeWatermarkTracker(final String vmPathEdgeId,
                                    final String lambdaPathEdgeId,
                                    final String taskId,
                                    final int parallelism) {
    this.lambdaWatermarkTracker = new R2SingleStageWatermarkTracker(parallelism, true);
    this.vmWatermarkTracker = new R2SingleStageWatermarkTracker(parallelism, false);
    this.dataFetcherWatermarkMap = new HashMap<>();
    this.dataFetcherWatermarkMap.put(lambdaPathEdgeId, 0L);
    this.dataFetcherWatermarkMap.put(vmPathEdgeId, 0L);

    this.lambdaPathEdgeId = lambdaPathEdgeId;
    this.vmPathEdgeId = vmPathEdgeId;
    this.parallelism = parallelism;

    this.taskId = taskId;
    this.lambdaPathTracker = new LambdaWatermarkTracker();
    this.vmPathTracker = new VMWatermarkTracker();
    this.bothPathTracker = new BothWatermarkTracker();

    this.watermarkTracker = vmWatermarkTracker;
  }

  private void setWatermarkTrackerAndPrevWatermark() {
    if (lambdaPathAllStopped) {
      watermarkTracker = vmPathTracker;
      prevWatermark = dataFetcherWatermarkMap.get(vmPathEdgeId);
    } else if (vmPathAllStopped) {
      watermarkTracker = lambdaPathTracker;
      prevWatermark = dataFetcherWatermarkMap.get(lambdaPathEdgeId);
    } else {
      watermarkTracker = bothPathTracker;
      // prevWatermark = Collections.min(dataFetcherWatermarkMap.values());
    }
  }

  public boolean stopInputIndex(final int taskIndex,
                                final String edgeId) {
    final int index = parallelism == 1 ? 0 : taskIndex;
    if (edgeId.equals(lambdaPathEdgeId)) {
      // start vm path
      // stop lambda path path
      LOG.info("Before Stop lambda path watermark index in task {}/{}/{}, vm: {}, lambda: {}, map: {}"
        , taskId, taskIndex, edgeId, vmWatermarkTracker, lambdaWatermarkTracker, dataFetcherWatermarkMap);

      // Stop lambda path
      if (lambdaWatermarkTracker.isStopped(index)) {
        LOG.warn("Edge already stopped but try to stop lambda edge " + edgeId + "/" + taskIndex  + " in " + taskId);
      } else {
        lambdaWatermarkTracker.stopInputPipeWatermark(index);
        if (!lambdaWatermarkTracker.updateAndGetCurrentWatermark().isPresent()) {
          lambdaPathAllStopped = true;
        }

        lambdaWatermarkTracker.updateAndGetCurrentWatermark()
          .ifPresent(watermark -> {
            dataFetcherWatermarkMap.put(lambdaPathEdgeId, watermark);
          });

        // Start vm path if possible to prevent two edges stopped at the same time
        vmPathAllStopped = false;

        // Set watermark
        vmWatermarkTracker.setWatermark(index, lambdaWatermarkTracker.getWatermark(index));
        vmWatermarkTracker.updateAndGetCurrentWatermark()
          .ifPresent(watermark -> {
            dataFetcherWatermarkMap.put(vmPathEdgeId, watermark);
          });
      }

      setWatermarkTrackerAndPrevWatermark();

      LOG.info("After Stop lambda path in task " + taskId + "/" + taskIndex + "/" + edgeId
        + "VM tracker: {}, Lambda tracker: {}, map: {}", vmWatermarkTracker, lambdaWatermarkTracker, dataFetcherWatermarkMap);
      return lambdaPathAllStopped;

    } else {
      LOG.info("Before Stop vm path watermark index in task {}/{}/{}, vm: {}, lambda: {}, map: {}",
        taskId, taskIndex, edgeId, vmWatermarkTracker, lambdaWatermarkTracker, dataFetcherWatermarkMap);

      // Stop vm path path
      if (vmWatermarkTracker.isStopped(index)) {
        LOG.warn("Edge already stopped but try to stop edge " + edgeId + "/" + taskIndex + " in " + taskId);
      } else {

        vmWatermarkTracker.stopInputPipeWatermark(index);

        if (!vmWatermarkTracker.updateAndGetCurrentWatermark().isPresent()) {
          vmPathAllStopped = true;
        }

        vmWatermarkTracker.updateAndGetCurrentWatermark()
          .ifPresent(watermark -> {
            dataFetcherWatermarkMap.put(vmPathEdgeId, watermark);
          });


        lambdaPathAllStopped = false;

        // Set watermark
        lambdaWatermarkTracker.setWatermark(index, vmWatermarkTracker.getWatermark(index));
        lambdaWatermarkTracker.updateAndGetCurrentWatermark()
          .ifPresent(watermark -> {
            dataFetcherWatermarkMap.put(lambdaPathEdgeId, watermark);
          });

      }

      setWatermarkTrackerAndPrevWatermark();

      LOG.info("After Stop vm path in task " + taskId + "/" + taskIndex + "/" + edgeId
        + "VM tracker: {}, Lambda tracker: {}, map: {}", vmWatermarkTracker, lambdaWatermarkTracker, dataFetcherWatermarkMap);
      return vmPathAllStopped;
    }
  }

  // this will be used for R3
  // return if all path is stopped
  public void startIndex(final int taskIndex,
                         final String edgeId,
                         final long watermark) {
    try {
      final int index = parallelism == 1 ? 0 : taskIndex;
      if (edgeId.equals(lambdaPathEdgeId)) {
        // start lambda path
        // stop vm path path
        LOG.info("Start lambda path watermark {} index in task {}/{}/{}, vm: {}, lambda: {}, map: {}",
          watermark,
          taskId, taskIndex, edgeId, vmWatermarkTracker, lambdaWatermarkTracker, dataFetcherWatermarkMap);
        lambdaPathAllStopped = false;

        if (lambdaWatermarkTracker.isStopped(index)) {
          lambdaWatermarkTracker.startInputPipeWatermark(index);
          lambdaWatermarkTracker.setWatermark(index, watermark);
          lambdaWatermarkTracker.updateAndGetCurrentWatermark().ifPresent(wm -> {
            dataFetcherWatermarkMap.put(lambdaPathEdgeId, wm);
          });
        } else {
          LOG.warn("Lambda edge " + index + "/" + edgeId + " started in "
            + taskId + " but try to start again");
        }
      } else {
        // start vm path
        // stop lambda path path
        LOG.info("Start vm path watermark  {} index in task {}/{}/{}, vm: {}, lambda: {}, map: {}"
          ,watermark
          , taskId, taskIndex, edgeId, vmWatermarkTracker, lambdaWatermarkTracker, dataFetcherWatermarkMap);
        vmPathAllStopped = false;

        if (vmWatermarkTracker.isStopped(index)) {
          vmWatermarkTracker.startInputPipeWatermark(index);
          vmWatermarkTracker.setWatermark(index, watermark);
          vmWatermarkTracker.updateAndGetCurrentWatermark().ifPresent(wm -> {
            dataFetcherWatermarkMap.put(vmPathEdgeId, wm);
          });
        } else {
          LOG.warn("VM edge " + index + "/" + edgeId + " started in "
            + taskId + " but try to start again");
        }

        LOG.info("After Start vm path in task " + taskId + "/" + taskIndex + "/" + edgeId
          + "VM tracker: {}, Lambda tracker: {}, map: {}", vmWatermarkTracker, lambdaWatermarkTracker, dataFetcherWatermarkMap);
      }
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Exception in starting edge index at " + taskId + " / edge " + edgeId + "/" + taskIndex);
    }

    setWatermarkTrackerAndPrevWatermark();
  }

  @Override
  public Optional<Long> trackAndEmitWatermarks(final String taskId,
                                               final String edgeId,
                                               final int taskIndex,
                                               final long watermark) {

    try {
      return watermarkTracker.trackAndEmitWatermarks(taskId, edgeId, taskIndex, watermark);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Exception while processing watermark "
        + taskId + "/" + edgeId + "/" + taskIndex + " watermark " + watermark
      + " logging " + toString());
    }
  }

  @Override
  public String toString() {
    return "LambdaPathAllStopped: " + lambdaPathAllStopped + ", "  +
      "VMPathAllStopped: " + vmPathAllStopped + ", " +
      "DataFetcherWatermarkMap: " + dataFetcherWatermarkMap + ", " +
      "PrevW: " + prevWatermark
      + "LambdaEdge: " + lambdaPathEdgeId
      + " VMEdge: " + vmPathEdgeId;
  }

  final class LambdaWatermarkTracker implements WatermarkTracker {

    @Override
    public Optional<Long> trackAndEmitWatermarks(String taskId, String edgeId, int edgeIndex, long watermark) {
      final Optional<Long> val = lambdaWatermarkTracker
        .trackAndEmitWatermarks(taskId, lambdaPathEdgeId, edgeIndex, watermark);

      if (val.isPresent()) {
        dataFetcherWatermarkMap.put(edgeId, val.get());
        if (val.get() < prevWatermark) {
          return Optional.empty();
        } else {
          prevWatermark = val.get();
        }
      }

      return val;
    }
  }

  final class VMWatermarkTracker implements WatermarkTracker {

    @Override
    public Optional<Long> trackAndEmitWatermarks(String taskId, String edgeId, int edgeIndex, long watermark) {
      final Optional<Long> val =
        vmWatermarkTracker.trackAndEmitWatermarks(taskId, vmPathEdgeId, edgeIndex, watermark);

      if (val.isPresent()) {
        dataFetcherWatermarkMap.put(edgeId, val.get());
        if (val.get() < prevWatermark) {
          return Optional.empty();
        } else {
          prevWatermark = val.get();
        }
      }

      return val;
    }
  }

  final class BothWatermarkTracker implements WatermarkTracker {

    @Override
    public Optional<Long> trackAndEmitWatermarks(String taskId, String edgeId, int edgeIndex, long watermark) {

      final Optional<Long> val;
      if (edgeId.equals(lambdaPathEdgeId)) {
        val = lambdaWatermarkTracker.trackAndEmitWatermarks(taskId, lambdaPathEdgeId, edgeIndex, watermark);
      } else {
        val = vmWatermarkTracker.trackAndEmitWatermarks(taskId, vmPathEdgeId, edgeIndex, watermark);
      }

      if (val.isPresent()) {
        // update output watermark!
        final long outputW = dataFetcherWatermarkMap.get(edgeId);
        if (outputW > val.get()) {
          throw new RuntimeException("Output watermark of " + edgeId + " is greater than the emitted watermark " + outputW + ", " + val.get());
        }

        dataFetcherWatermarkMap.put(edgeId, val.get());

        /*
      LOG.info("R2 pair trackAndEmitWatermark task {} edge {} / {} / {} emit watermark {} prevW: {} " +
          "dataFetcherWatermarkMap {}",
        taskId, edgeId, taskIndex, watermark, outputW, prevWatermark, dataFetcherWatermarkMap);
        */

        final long minWatermark = Collections.min(dataFetcherWatermarkMap.values());
        if (minWatermark < prevWatermark) {
          return Optional.empty();
        } else {
          prevWatermark = minWatermark;
          return Optional.of(minWatermark);
        }
      } else {
        return Optional.empty();
      }
    }
  }
}
