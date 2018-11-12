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

import net.jcip.annotations.ThreadSafe;

import org.apache.nemo.conf.JobConf;

import org.apache.nemo.runtime.executor.datatransfer.InputReader;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.Serializable;


/**
 * Used by tasks to get/fetch (probably remote) broadcast variables.
 */
@ThreadSafe
public final class LambdaBroadcastManagerWorkerImpl implements BroadcastManagerWorker {
  private static final Logger LOG = LoggerFactory.getLogger(LambdaBroadcastManagerWorkerImpl.class.getName());
  private final String executorId;

  /**
   * Initializes the cache for broadcast variables.
   * This cache handles concurrent cache operations by multiple threads, and is able to fetch data from
   * remote executors or the master.
   *
   * @param executorId of the executor.
   */
  @Inject
  private LambdaBroadcastManagerWorkerImpl(
    @Parameter(JobConf.ExecutorId.class) final String executorId) {
    this.executorId = executorId;
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
    // do nothing
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
    // get from lambda
    return null;
  }
}
