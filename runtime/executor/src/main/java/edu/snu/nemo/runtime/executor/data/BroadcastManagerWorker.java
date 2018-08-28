/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.runtime.executor.data;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.ByteString;
import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.runtime.common.RuntimeIdManager;
import edu.snu.nemo.runtime.common.comm.ControlMessage;
import edu.snu.nemo.runtime.common.message.MessageEnvironment;
import edu.snu.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import edu.snu.nemo.runtime.executor.datatransfer.InputReader;
import net.jcip.annotations.ThreadSafe;
import org.apache.commons.lang.SerializationUtils;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 */
@ThreadSafe
public final class BroadcastManagerWorker {
  private static final Logger LOG = LoggerFactory.getLogger(BroadcastManagerWorker.class.getName());
  private static BroadcastManagerWorker staticReference;

  private final ConcurrentHashMap<Serializable, InputReader> tagToReader;
  private final LoadingCache<Serializable, Object> tagToVariableCache;

  @Inject
  public BroadcastManagerWorker(@Parameter(JobConf.ExecutorId.class) final String executorId,
                                final PersistentConnectionToMasterMap toMaster) {
    staticReference = this;
    this.tagToReader = new ConcurrentHashMap<>();
    this.tagToVariableCache = CacheBuilder.newBuilder()
      .maximumSize(100)
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .build(
        new CacheLoader<Serializable, Object>() {
          public Object load(final Serializable tag) throws Exception {
            LOG.info("Start to load broadcast {}", tag.toString());
            if (tagToReader.containsKey(tag)) {
              // Get from reader
              final InputReader inputReader = tagToReader.get(tag);
              final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> iterators = inputReader.read();
              if (iterators.size() != 1) {
                throw new IllegalStateException(tag.toString());
              }
              final DataUtil.IteratorWithNumBytes iterator = iterators.get(0).get();
              if (!iterator.hasNext()) {
                throw new IllegalStateException(tag.toString() + " (no element) " + iterator.toString());
              }
              final Object result = iterator.next();
              if (iterator.hasNext()) {
                throw new IllegalStateException(tag.toString() + " (more than single element) " + iterator.toString());
              }
              return result;
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
                        .setTag(ByteString.copyFrom(SerializationUtils.serialize(tag)))
                        .build())
                    .build());
              return SerializationUtils.deserialize(
                responseFromMasterFuture.get().getBroadcastVariableMsg().getVariabe().toByteArray());
            }
          }
        });
  }

  /**
   * @param tag
   * @param inputReader
   */
  public void registerInputReader(final Serializable tag,
                                  final InputReader inputReader) {
    this.tagToReader.put(tag, inputReader);
  }

  public Object get(final Serializable tag)  {
    // catch exceptions (e.g., read exceptions)
    try {
      return tagToVariableCache.get(tag);
    } catch (ExecutionException e) {
      throw new IllegalStateException(e);
    }
  }

  public static BroadcastManagerWorker getStaticReference() {
    return staticReference;
  }
}
