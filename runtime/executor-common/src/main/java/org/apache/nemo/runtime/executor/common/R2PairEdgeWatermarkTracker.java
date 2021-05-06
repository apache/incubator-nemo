package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.Util;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.*;

public final class R2PairEdgeWatermarkTracker implements WatermarkTracker {
  private static final Logger LOG = LoggerFactory.getLogger(R2PairEdgeWatermarkTracker.class.getName());

  private long prevWatermark = 0L;

  private final String lambdaPathEdgeId;
  private final String vmPathEdgeId;
  private final int parallelism;

  private final R2SingleStageWatermarkTracker lambdaWatermarkTracker;
  private final R2SingleStageWatermarkTracker vmWatermarkTracker;

  private long vmWatermark;
  private long lambdaWatermark;

  private final String taskId;

  private boolean vmPathAllStopped = false;
  private boolean lambdaPathAllStopped = true;

  private WatermarkTracker watermarkTracker;

  private final WatermarkTracker lambdaPathTracker;
  private final WatermarkTracker vmPathTracker;
  private final WatermarkTracker bothPathTracker;

  private final boolean logging;

  public R2PairEdgeWatermarkTracker(final String vmPathEdgeId,
                                    final String lambdaPathEdgeId,
                                    final String taskId,
                                    final boolean logging,
                                    final int parallelism) {
    this.lambdaWatermarkTracker = new R2SingleStageWatermarkTracker(parallelism, true);
    this.vmWatermarkTracker = new R2SingleStageWatermarkTracker(parallelism, false);
    this.vmWatermark = 0;
    this.lambdaWatermark = 0;
    this.logging = logging;

    this.lambdaPathEdgeId = lambdaPathEdgeId;
    this.vmPathEdgeId = vmPathEdgeId;
    this.parallelism = parallelism;

    this.taskId = taskId;
    this.lambdaPathTracker = new LambdaWatermarkTracker();
    this.vmPathTracker = new VMWatermarkTracker();
    this.bothPathTracker = new BothWatermarkTracker();

    this.watermarkTracker = vmPathTracker;
  }

  private R2PairEdgeWatermarkTracker(final String vmPathEdgeId,
                                     final String lambdaPathEdgeId,
                                     final String taskId,
                                     final boolean logging,
                                     final int parallelism,
                                     final long prevWatermark,
                                     final R2SingleStageWatermarkTracker lambdaWatermarkTracker,
                                     final R2SingleStageWatermarkTracker vmWatermarkTracker,
                                     final long vmWatermark,
                                     final long lambdaWatermark,
                                     final boolean vmPathAllStopped,
                                     final boolean lambdaPathAllStopped) {
    this.prevWatermark = prevWatermark;
    this.lambdaPathEdgeId = lambdaPathEdgeId;
    this.logging = logging;
    this.vmPathEdgeId = vmPathEdgeId;
    this.parallelism = parallelism;
    this.lambdaWatermarkTracker = lambdaWatermarkTracker;
    this.vmWatermarkTracker = vmWatermarkTracker;
    this.vmWatermark = vmWatermark;
    this.lambdaWatermark = lambdaWatermark;
    this.taskId = taskId;
    this.vmPathAllStopped = vmPathAllStopped;
    this.lambdaPathAllStopped = lambdaPathAllStopped;
    this.lambdaPathTracker = new LambdaWatermarkTracker();
    this.vmPathTracker = new VMWatermarkTracker();
    this.bothPathTracker = new BothWatermarkTracker();

    if (lambdaPathAllStopped) {
      this.watermarkTracker = vmPathTracker;
    } else if (vmPathAllStopped) {
      this.watermarkTracker = lambdaPathTracker;
    } else {
      this.watermarkTracker = bothPathTracker;
    }
  }

  public static R2PairEdgeWatermarkTracker decode(final String vmPathEdgeId,
                                                  final String lambdaPathEdgeId,
                                                  final String taskId,
                                                  final DataInputStream dis) {
    try {
      final boolean logging = dis.readBoolean();
      final int parallelism = dis.readInt();
      final long prevWatermark = dis.readLong();
      final R2SingleStageWatermarkTracker lambdaWatermarkTracker = R2SingleStageWatermarkTracker.decode(taskId, dis);
      final R2SingleStageWatermarkTracker vmWatermarkTracker = R2SingleStageWatermarkTracker.decode(taskId, dis);
      final long vmWatermark = dis.readLong();
      final long lambdaWatermark = dis.readLong();
      final boolean vmPathAllStopped = dis.readBoolean();
      final boolean lambdaPathAllStopped = dis.readBoolean();

      return new R2PairEdgeWatermarkTracker(vmPathEdgeId, lambdaPathEdgeId,
        taskId, logging, parallelism, prevWatermark, lambdaWatermarkTracker, vmWatermarkTracker,
        vmWatermark, lambdaWatermark, vmPathAllStopped, lambdaPathAllStopped);

    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void encode(final DataOutputStream dos) {
    try {
      dos.writeBoolean(logging);
      dos.writeInt(parallelism);
      dos.writeLong(prevWatermark);
      // lambda watermark tracker
      lambdaWatermarkTracker.encode(taskId, dos);
      // vm watermark tracker
      vmWatermarkTracker.encode(taskId, dos);
      dos.writeLong(vmWatermark);
      dos.writeLong(lambdaWatermark);
      dos.writeBoolean(vmPathAllStopped);
      dos.writeBoolean(lambdaPathAllStopped);

    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private void setWatermarkTrackerAndPrevWatermark() {
    if (lambdaPathAllStopped) {
      watermarkTracker = vmPathTracker;
      prevWatermark = vmWatermark;
    } else if (vmPathAllStopped) {
      watermarkTracker = lambdaPathTracker;
      prevWatermark = lambdaWatermark;
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
      if (logging) {
        LOG.info("Before Stop lambda path watermark index in task {}/{}/{}, vm: {}, lambda: {}, vm watermark: {}, " +
            "lambda watermark {}, prevEmitWatermark: {}"
          , taskId, taskIndex, edgeId, vmWatermarkTracker, lambdaWatermarkTracker,
          new Instant(vmWatermark), new Instant(lambdaWatermark),
          new Instant(prevWatermark));
      }

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
            lambdaWatermark = watermark;
          });

        // Start vm path if possible to prevent two edges stopped at the same time
        vmPathAllStopped = false;

        // Set watermark
        // vmWatermarkTracker.setWatermark(index, lambdaWatermarkTracker.getWatermark(index));
        // vmWatermarkTracker.updateAndGetCurrentWatermark()
        //  .ifPresent(watermark -> {
        //    vmWatermark = watermark;
        //  });
      }

      setWatermarkTrackerAndPrevWatermark();

      if (logging) {
        LOG.info("After Stop lambda path in task " + taskId + "/" + taskIndex + "/" + edgeId
            + "VM tracker: {}, Lambda tracker: {}, vm watermark: {}, " +
            "lambda watermark: {}, prevEmitWatermark: {}",
          vmWatermarkTracker, lambdaWatermarkTracker, new Instant(vmWatermark),
          new Instant(lambdaWatermark),
          new Instant(prevWatermark));
      }
      return lambdaPathAllStopped;

    } else {
      if (logging) {
        LOG.info("Before Stop vm path watermark index in task {}/{}/{}, vm: {}, lambda: {}, vm Watermark: {}, " +
            "lambda Watermark: {}, prevEmitWatermark: {}",
          taskId, taskIndex, edgeId, vmWatermarkTracker, lambdaWatermarkTracker,
          new Instant(vmWatermark),
          new Instant(lambdaWatermark),
          new Instant(prevWatermark));
      }

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
            vmWatermark = watermark;
          });


        lambdaPathAllStopped = false;

        // Set watermark
//        lambdaWatermarkTracker.setWatermark(index, vmWatermarkTracker.getWatermark(index));
//        lambdaWatermarkTracker.updateAndGetCurrentWatermark()
//          .ifPresent(watermark -> {
//            lambdaWatermark = watermark;
//          });

      }

      setWatermarkTrackerAndPrevWatermark();

      if (logging) {
        LOG.info("After Stop vm path in task " + taskId + "/" + taskIndex + "/" + edgeId
            + "VM tracker: {}, Lambda tracker: {}, vmw: {}, lw: {}, prevEmitWatermark: {}",
          vmWatermarkTracker, lambdaWatermarkTracker,
          new Instant(vmWatermark),
          new Instant(lambdaWatermark),
          new Instant(prevWatermark));
      }

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
        if (logging) {
          LOG.info("Start lambda path watermark {} index in task {}/{}/{}, vm: {}, lambda: {}, vmw: {}, lw: {}," +
              "prevEmitWatermark: {}",
            new Instant(watermark),
            taskId, taskIndex, edgeId, vmWatermarkTracker, lambdaWatermarkTracker,
            new Instant(vmWatermark), new Instant(lambdaWatermark),
            new Instant(prevWatermark));
        }

        lambdaPathAllStopped = false;

        if (lambdaWatermarkTracker.isStopped(index)) {
          lambdaWatermarkTracker.startInputPipeWatermark(index);
          lambdaWatermarkTracker.setWatermark(index, watermark, taskId, edgeId);
          lambdaWatermarkTracker.updateAndGetCurrentWatermark().ifPresent(wm -> {
            lambdaWatermark = wm;
          });
        } else {
          LOG.warn("Lambda edge " + index + "/" + edgeId + " started in "
            + taskId + " but try to start again");
        }
      } else {
        // start vm path
        // stop lambda path path
        if (logging) {
          LOG.info("Start vm path watermark  {} index in task {}/{}/{}, vm: {}, lambda: {}, vmw: {}, lw: {}, " +
              "prevEmitWatermark: {}"
            , watermark
            , taskId, taskIndex, edgeId, vmWatermarkTracker, lambdaWatermarkTracker,
            new Instant(vmWatermark), new Instant(lambdaWatermark),
            new Instant(prevWatermark));
        }

        vmPathAllStopped = false;

        if (vmWatermarkTracker.isStopped(index)) {
          vmWatermarkTracker.startInputPipeWatermark(index);
          vmWatermarkTracker.setWatermark(index, watermark, taskId, edgeId);
          vmWatermarkTracker.updateAndGetCurrentWatermark().ifPresent(wm -> {
            vmWatermark = wm;
          });
        } else {
          LOG.warn("VM edge " + index + "/" + edgeId + " started in "
            + taskId + " but try to start again");
        }

        if (logging) {
          LOG.info("After Start vm path in task " + taskId + "/" + taskIndex + "/" + edgeId
              + "VM tracker: {}, Lambda tracker: {}, vmw: {}, lw: {}, prevEmitWatermark: {}", vmWatermarkTracker,
            lambdaWatermarkTracker, new Instant(vmWatermark), new Instant(lambdaWatermark),
            new Instant(prevWatermark));
        }
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
        + taskId + "/" + edgeId + "/" + taskIndex + " watermark " + new Instant(watermark)
      + " logging " + toString());
    }
  }

  @Override
  public String toString() {
    return "LambdaPathAllStopped: " + lambdaPathAllStopped + ", "  +
      "VMPathAllStopped: " + vmPathAllStopped + ", " +
      "VMWatermark: " + new Instant(vmWatermark) + ", " +
      "LambdaWatermark: " + new Instant(lambdaWatermark) + ", " +
      "PrevW: " + new Instant(prevWatermark)
      + "LambdaEdge: " + lambdaPathEdgeId
      + " VMEdge: " + vmPathEdgeId;
  }

  final class LambdaWatermarkTracker implements WatermarkTracker {

    @Override
    public Optional<Long> trackAndEmitWatermarks(String taskId, String edgeId, int edgeIndex, long watermark) {
      final Optional<Long> val = lambdaWatermarkTracker
        .trackAndEmitWatermarks(taskId, lambdaPathEdgeId, edgeIndex, watermark);

      if (val.isPresent()) {
        lambdaWatermark = val.get();
        if (val.get() <= prevWatermark) {
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
        vmWatermark = val.get();
        if (val.get() <= prevWatermark) {
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

        if (val.isPresent()) {
          if (lambdaWatermark > val.get()) {
            throw new RuntimeException("Output watermark of " + edgeId + " is greater than the emitted watermark " +
              new Instant(lambdaWatermark) + ", " + new Instant(val.get()));
          }
          lambdaWatermark = val.get();
        }
      } else {
        val = vmWatermarkTracker.trackAndEmitWatermarks(taskId, vmPathEdgeId, edgeIndex, watermark);

        if (val.isPresent()) {
          if (vmWatermark > val.get()) {
            throw new RuntimeException("Output watermark of " + edgeId + " is greater than the emitted watermark " +
              new Instant(vmWatermark) + ", " + new Instant(val.get()));
          }
          vmWatermark = val.get();
        }
      }

      final long minWatermark = vmWatermark < lambdaWatermark ? vmWatermark : lambdaWatermark;
      if (minWatermark <= prevWatermark) {
        return Optional.empty();
      } else {
        prevWatermark = minWatermark;
        return Optional.of(minWatermark);
      }
    }
  }
}
