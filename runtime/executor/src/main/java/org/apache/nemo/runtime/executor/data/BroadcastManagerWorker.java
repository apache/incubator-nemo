/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.executor.data;

import com.google.protobuf.ByteString;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.executor.datatransfer.InputReader;
import net.jcip.annotations.ThreadSafe;
import org.apache.commons.lang.SerializationUtils;
import org.apache.nemo.runtime.executor.datatransfer.WatermarkWithIndex;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Used by tasks to get/fetch (probably remote) broadcast variables.
 */
@ThreadSafe
public final class BroadcastManagerWorker {
  private static final Logger LOG = LoggerFactory.getLogger(BroadcastManagerWorker.class.getName());
  private static BroadcastManagerWorker staticReference;

  private final ConcurrentHashMap<Serializable, InputReader> idToReader;
  private final ConcurrentMap<Serializable, Map<Object, Object>> idToVariableCache;
  private final Map<InputReader, DataUtil.IteratorWithNumBytes> readerIteratorMap;
  private final PersistentConnectionToMasterMap toMaster;
  private final String executorId;

  /**
   * Initializes the cache for broadcast variables.
   * This cache handles concurrent cache operations by multiple threads, and is able to fetch data from
   * remote executors or the master.
   *
   * @param executorId of the executor.
   * @param toMaster connection.
   */
  @Inject
  private BroadcastManagerWorker(@Parameter(JobConf.ExecutorId.class) final String executorId,
                                final PersistentConnectionToMasterMap toMaster) {
    staticReference = this;
    this.idToReader = new ConcurrentHashMap<>();
    this.idToVariableCache = new ConcurrentHashMap<>();
    this.executorId = executorId;
    this.toMaster = toMaster;
    this.readerIteratorMap = new HashMap<>();
  }

  /**
   * When the broadcast variable can be read by an input reader.
   * (i.e., the variable is expressed as an IREdge, and reside in a executor as a block)
   *
   * @param id of the broadcast variable.
   * @param inputReader the {@link InputReader} to register.
   */
  public void registerInputReader(final Serializable id,
                                  final InputReader inputReader) {
    this.idToReader.put(id, inputReader);
  }

  public Object get(final Serializable id) {
    return get(id, null);
  }

  /**
   * Get the variable with the id.
   * @param id of the variable.
   * @return the variable.
   */
  public Object get(final Serializable id, final Object key) {
    LOG.info("get/key {}/{}", id, key);
    idToVariableCache.putIfAbsent(id, new HashMap<>());
    final Map<Object, Object> map = idToVariableCache.get(id);
    final InputReader inputReader = idToReader.get(id);

    synchronized (inputReader) {
      if (inputReader != null) {
        LOG.info("Start to load key {} of ser {}", key, id);

        if (map.get(key) != null) {
          return map.get(key);
        }

        if (readerIteratorMap.get(inputReader) == null) {
          final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> iterators = inputReader.read();
          if (iterators.size() != 1) {
            throw new IllegalStateException(key.toString());
          }

          final DataUtil.IteratorWithNumBytes it;
          try {
            it = iterators.get(0).get();
            readerIteratorMap.put(inputReader, it);
          } catch (final Exception e) {
            throw new RuntimeException(e);
          }
        }

        final DataUtil.IteratorWithNumBytes iterator = readerIteratorMap.get(inputReader);

        LOG.info("Before hasNext");
        if (!iterator.hasNext()) {
          throw new IllegalStateException(key.toString() + " (no element) " + iterator.toString());
        }

        while (iterator.hasNext()) {
          final Object result = iterator.next();
          LOG.info("After hasNext, result: {}", result);

          // ad-hoc
          if (result instanceof WatermarkWithIndex) {
            continue;
          }

          // ad-hoc
          if (result instanceof WindowedValue) {
            final WindowedValue wv = (WindowedValue) result;
            for (final Object w : wv.getWindows()) {
              LOG.info("Put value {} for window {}", wv, w);
              map.putIfAbsent(w, wv);
            }
          } else {
            LOG.info("Put value {}", result);
            map.put(key, result);
          }

          if (map.get(key) == null) {
            continue;
          }

          return map.get(key);
        }

        throw new IllegalStateException("Iterator always returns true");
      } else {
        // Get from master
        final CompletableFuture<ControlMessage.Message> responseFromMasterFuture = toMaster
          .getMessageSender(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID).request(
            ControlMessage.Message.newBuilder()
              .setId(RuntimeIdManager.generateMessageId())
              .setListenerId(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
              .setType(ControlMessage.MessageType.RequestBroadcastVariable)
              .setRequestbroadcastVariableMsg(
                ControlMessage.RequestBroadcastVariableMessage.newBuilder()
                  .setExecutorId(executorId)
                  .setBroadcastId(ByteString.copyFrom(SerializationUtils.serialize(id)))
                  .build())
              .build());
        try {
          return SerializationUtils.deserialize(
            responseFromMasterFuture.get().getBroadcastVariableMsg().getVariable().toByteArray());
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * @return the static reference for those that do not use TANG and cannot access the singleton object.
   */
  public static BroadcastManagerWorker getStaticReference() {
    return staticReference;
  }
}
