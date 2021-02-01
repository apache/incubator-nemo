package org.apache.nemo.runtime.executor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.TaskMetrics;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.client.StreamingLambdaWorkerProxy;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.offloading.common.OffloadingTransform;
import org.apache.nemo.offloading.common.OffloadingWorker;
import org.apache.nemo.offloading.common.OffloadingWorkerFactory;
import org.apache.nemo.runtime.executor.common.OffloadingManager;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.nemo.runtime.executor.common.TaskHandlingEvent;
import org.apache.nemo.runtime.lambdaexecutor.*;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingTask;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class OffloadingManagerImpl implements OffloadingManager {
  private static final Logger LOG = LoggerFactory.getLogger(OffloadingManagerImpl.class.getName());

  // private final List<OffloadingWorker> workers;
  private final List<OffloadingWorker> workers;
  private final OffloadingWorkerFactory workerFactory;
  private final ConcurrentMap<String, TaskExecutor> offloadedTaskMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, TinyTaskWorker> deletePendingWorkers = new ConcurrentHashMap<>();


  @Inject
  private OffloadingManagerImpl(OffloadingWorkerFactory workerFactory) {
    this.workerFactory = workerFactory;
    this.workers = new LinkedList<>();
  }

  public void createWorker(final int num) {
    // workerFactory.createStreamingWorker()
  }

  @Override
  public void offloading(String taskId, byte[] serializedDag) {

  }

  @Override
  public void writeData(String taskId, TaskHandlingEvent data) {

  }
}
