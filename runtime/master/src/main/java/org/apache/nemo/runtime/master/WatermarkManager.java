package org.apache.nemo.runtime.master;

import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.Stage;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.runtime.common.plan.Task;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;

public final class WatermarkManager {
  private static final Logger LOG = LoggerFactory.getLogger(WatermarkManager.class.getName());

  private final ConcurrentMap<String, Long> stageOutputWatermarkMap;
  private final ConcurrentMap<String, Long> stageInputWatermarkMap;

  private final Map<String, Stage> stageIdStageMap;
  private final Map<String, StageWatermarkTracker> stageWatermarkTrackerMap;
  private final Map<String, Boolean> inputWatermarkUpdateRequiredMap;

  private DAG<Stage, StageEdge> stageDag;
  private final ScheduledExecutorService scheduler;

  @Inject
  private WatermarkManager() {
    this.stageOutputWatermarkMap = new ConcurrentHashMap<>();
    this.stageInputWatermarkMap = new ConcurrentHashMap<>();
    this.inputWatermarkUpdateRequiredMap = new ConcurrentHashMap<>();

    this.stageWatermarkTrackerMap = new HashMap<>();
    this.stageIdStageMap = new HashMap<>();

    this.scheduler = Executors.newSingleThreadScheduledExecutor();
    scheduler.scheduleAtFixedRate(() -> {

      LOG.info("Output watermarks {}", stageOutputWatermarkMap);
      LOG.info("Input watermarks {}", stageInputWatermarkMap);

    }, 1, 1, TimeUnit.SECONDS);
  }

  public void setStage(final DAG<Stage, StageEdge> dag) {
    stageDag = dag;
    for (final Stage stage : dag.getVertices()) {
      stageIdStageMap.put(stage.getId(), stage);
      stageWatermarkTrackerMap.put(stage.getId(), new StageWatermarkTracker(stage.getParallelism()));
      stageOutputWatermarkMap.put(stage.getId(), 0L);
      stageInputWatermarkMap.put(stage.getId(), 0L);
      inputWatermarkUpdateRequiredMap.put(stage.getId(), false);
    }
  }

  public void updateWatermark(final String taskId, final long watermark) {
    final int index = RuntimeIdManager.getIndexFromTaskId(taskId);
    final String stageId = RuntimeIdManager.getStageIdFromTaskId(taskId);


    final StageWatermarkTracker stageWatermarkTracker = stageWatermarkTrackerMap.get(stageId);

    if (stageId.equals("Stage2")) {
      LOG.info("request update watermark of stage2 \n {}", stageWatermarkTracker);
    }

    final Optional<Long> val = stageWatermarkTracker.trackAndEmitWatermarks(index, watermark);

    if (val.isPresent()) {
      // update output watermark!
      final long outputW = stageOutputWatermarkMap.get(stageId);
      if (outputW > val.get()) {
        throw new RuntimeException("Output watermark of " + stageId + " is greater than the emitted watermark " + outputW + ", " + val.get());
      }

    if (stageId.equals("Stage2")) {
      LOG.info("set update watermark of stage2 ");
    }

      stageOutputWatermarkMap.put(stageId, val.get());
      // update dependent stages watermarks
      final Stage stage = stageIdStageMap.get(stageId);
      stageDag.getOutgoingEdgesOf(stage).forEach(edge -> {
        final Stage child = edge.getDst();
        synchronized (child) {
          inputWatermarkUpdateRequiredMap.put(child.getId(), true);
        }
      });
    }
  }


  private boolean isOneToOne(final Stage stage) {

    for (final StageEdge edge : stageDag.getIncomingEdgesOf(stage)) {
      if (edge.getDataCommunicationPattern().equals(CommunicationPatternProperty.Value.OneToOne)) {
        return true;
      }
    }

    return false;
  }

  public long getInputWatermark(final String taskId) {
    final String stageId = RuntimeIdManager.getStageIdFromTaskId(taskId);
    //LOG.info("Get input watermark {}", stageId);
    final long currInputWatermark = stageInputWatermarkMap.get(stageId);
    final Stage stage = stageIdStageMap.get(stageId);
    final boolean oneToOne = isOneToOne(stage);

    //LOG.info("onetoone ? {}: {}, {}", stageId, oneToOne, taskId);
    synchronized (stage) {

      if (oneToOne) {
        final int index = RuntimeIdManager.getIndexFromTaskId(taskId);
        long minWatermark = Long.MAX_VALUE;
        for (final StageEdge edge : stageDag.getIncomingEdgesOf(stage)) {
          final Stage parent = edge.getSrc();
          final StageWatermarkTracker tracker = stageWatermarkTrackerMap.get(parent.getId());
          final long taskW = tracker.getWatermark(index);
          if (taskW < minWatermark) {
            minWatermark = taskW;
          }
        }
        return minWatermark;
      } else {
        if (inputWatermarkUpdateRequiredMap.get(stageId)) {
          // update input watermark
          inputWatermarkUpdateRequiredMap.put(stageId, false);

          long minWatermark = Long.MAX_VALUE;
          for (final StageEdge edge : stageDag.getIncomingEdgesOf(stage)) {
            final Stage parent = edge.getSrc();
            final long stageW = stageOutputWatermarkMap.get(parent.getId());
            if (stageW < minWatermark) {
              minWatermark = stageW;
            }
          }

          // minWAtermar ==> stage input watermark
          if (minWatermark < currInputWatermark) {
            throw new RuntimeException("Input watermark cannot be less than prev input watermark: " + minWatermark + ", "
              + currInputWatermark + ", " + " for stage " + stageId);
          }
          stageInputWatermarkMap.put(stageId, minWatermark);

          //LOG.info("Watermark for {}: {}", taskId, new Instant(minWatermark));
          return minWatermark;
        } else {
          //LOG.info("Watermark for {}: {}", taskId, new Instant(currInputWatermark));
          return currInputWatermark;
        }
      }
    }
  }


  private final class StageWatermarkTracker {

    private final List<Long> watermarks;
    private int minWatermarkIndex;
    private Long currMinWatermark = Long.MIN_VALUE;

    public StageWatermarkTracker(final int numTasks) {
      this.watermarks = new ArrayList<>(numTasks);
      this.minWatermarkIndex = 0;

      for (int i = 0; i < numTasks; i++) {
        watermarks.add(Long.MIN_VALUE);
      }
    }

    private int findNextMinWatermarkIndex() {
      int index = -1;
      long timestamp = Long.MAX_VALUE;
      for (int i = 0; i < watermarks.size(); i++) {
        if (watermarks.get(i) < timestamp) {
          index = i;
          timestamp = watermarks.get(i);
        }
      }
      return index;
    }

    public synchronized long getWatermark(final int index) {
      //LOG.info("Watermark request index: {}. size: {},. get {}",
      //  index, watermarks.size(), watermarks.get(index));
      return watermarks.get(index);
    }

    public synchronized Optional<Long> trackAndEmitWatermarks(final int edgeIndex, final long watermark) {
      if (edgeIndex == minWatermarkIndex) {
        // update min watermark
        watermarks.set(minWatermarkIndex, watermark);

        // find min watermark
        final int nextMinWatermarkIndex = findNextMinWatermarkIndex();
        final Long nextMinWatermark = watermarks.get(nextMinWatermarkIndex);

        if (nextMinWatermark <= currMinWatermark) {
          // it is possible
          minWatermarkIndex = nextMinWatermarkIndex;
          //LOG.warn("{} watermark less than prev: {}, {} maybe due to the new edge index",
          //  vertex.getId(), new Instant(currMinWatermark.getTimestamp()), new Instant(nextMinWatermark.getTimestamp()));
        } else if (nextMinWatermark > currMinWatermark) {
          // Watermark timestamp progress!
          // Emit the min watermark
          minWatermarkIndex = nextMinWatermarkIndex;
          currMinWatermark = nextMinWatermark;
          return Optional.of(currMinWatermark);
        }
      } else {
        // The recent watermark timestamp cannot be less than the previous one
        // because watermark is monotonically increasing.
        if (watermarks.get(edgeIndex) > watermark) {

        } else {
          watermarks.set(edgeIndex, watermark);
        }
      }

      return Optional.empty();
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append("[");
      for (int i = 0; i < watermarks.size(); i++) {
        sb.append(i);
        sb.append(": ");
        sb.append(watermarks.get(i));
        sb.append("\n");
      }
      sb.append("]");
      return sb.toString();
    }
  }
}
