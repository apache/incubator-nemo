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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.ByteString;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import net.jcip.annotations.ThreadSafe;
import org.apache.commons.lang.SerializationUtils;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Used by tasks to get/fetch (probably remote) broadcast variables.
 */
@ThreadSafe
public final class BroadcastManagerWorker {
  private static final Logger LOG = LoggerFactory.getLogger(BroadcastManagerWorker.class.getName());
  private static BroadcastManagerWorker staticReference;

  private final LoadingCache<Serializable, Object> idToVariableCache;

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
    this.idToVariableCache = CacheBuilder.newBuilder()
      .maximumSize(100)
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .build(
        new CacheLoader<Serializable, Object>() {
          public Object load(final Serializable id) throws Exception {
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
            return SerializationUtils.deserialize(
              responseFromMasterFuture.get().getBroadcastVariableMsg().getVariable().toByteArray());
          }
        });
  }


  /**
   * Get the variable with the id.
   * @param id of the variable.
   * @return the variable.
   */
  public Object get(final Serializable id)  {
    LOG.info("get {}", id);
    try {
      return idToVariableCache.get(id);
    } catch (ExecutionException e) {
      // TODO #207: Handle broadcast variable fetch exceptions
      throw new IllegalStateException(e);
    }
  }

  /**
   * @return the static reference for those that do not use TANG and cannot access the singleton object.
   */
  public static BroadcastManagerWorker getStaticReference() {
    return staticReference;
  }
}
