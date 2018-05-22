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

import edu.snu.nemo.conf.JobConf;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A class to restrict parallel connection per runtime edge.
 */
public final class ParallelConnectionRestrictor {
  private static final Object FUTURE_OBJECT = new Object();
  private final Map<String, Integer> runtimeEdgeIdToNumOutstandingConnections = new ConcurrentHashMap<>();
  private final Map<String, Queue<CompletableFuture<Object>>> runtimeEdgeIdToPendingConnections
      = new ConcurrentHashMap<>();
  private final int maxNum;

  @Inject
  private ParallelConnectionRestrictor(@Parameter(JobConf.MaxNumDownloadsForARuntimeEdge.class) final int maxNum) {
    this.maxNum = maxNum;
  }

  /**
   * Request a new connection request.
   * @param runtimeEdgeId the corresponding runtime edge id.
   * @return a future that will be completed when the connection is granted.
   */
  public synchronized CompletableFuture<Object> newConnectionRequest(final String runtimeEdgeId) {
    runtimeEdgeIdToNumOutstandingConnections.putIfAbsent(runtimeEdgeId, 0);
    runtimeEdgeIdToPendingConnections.computeIfAbsent(runtimeEdgeId, id -> new ArrayDeque<>());
    final CompletableFuture<Object> future = new CompletableFuture();
    final int currentOutstandingConnections = runtimeEdgeIdToNumOutstandingConnections.get(runtimeEdgeId);

    if (currentOutstandingConnections < maxNum) {
      // grant immediately
      future.complete(FUTURE_OBJECT);
      runtimeEdgeIdToNumOutstandingConnections.put(runtimeEdgeId, currentOutstandingConnections + 1);
    } else {
      // add to pending queue
      runtimeEdgeIdToPendingConnections.get(runtimeEdgeId).add(future);
    }
    return future;
  }

  /**
   * Indicates the connection has finished.
   * @param runtimeEdgeId the corresponding runtime edge id.
   */
  public synchronized void connectionFinished(final String runtimeEdgeId) {
    final Queue<CompletableFuture<Object>> pendingConnections = runtimeEdgeIdToPendingConnections.get(runtimeEdgeId);
    if (pendingConnections.size() == 0) {
      final int currentOutstandingConnections = runtimeEdgeIdToNumOutstandingConnections.get(runtimeEdgeId);
      runtimeEdgeIdToNumOutstandingConnections.put(runtimeEdgeId, currentOutstandingConnections - 1);
    } else {
      final CompletableFuture<Object> nextFuture = pendingConnections.poll();
      nextFuture.complete(FUTURE_OBJECT);
    }
  }
}
