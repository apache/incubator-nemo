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
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/**
 * A class to restrict parallel connection per runtime edge.
 * Executors can suffer from performance degradation and network-related exceptions when there are massive connections,
 * especially under low network bandwidth or high volume of data.
 */
public final class BlockTransferConnectionQueue {
  private final Map<String, Integer> runtimeEdgeIdToNumCurrentConnections = new HashMap<>();
  private final Map<String, Queue<CompletableFuture<Void>>> runtimeEdgeIdToPendingConnections = new HashMap<>();
  private final int maxNum;

  @Inject
  private BlockTransferConnectionQueue(@Parameter(JobConf.MaxNumDownloadsForARuntimeEdge.class) final int maxNum) {
    this.maxNum = maxNum;
  }

  /**
   * Request a permission to make a connection.
   * @param runtimeEdgeId the corresponding runtime edge id.
   * @return a future that will be completed when the connection is granted.
   */
  public synchronized CompletableFuture<Void> requestConnectPermission(final String runtimeEdgeId) {
    runtimeEdgeIdToNumCurrentConnections.putIfAbsent(runtimeEdgeId, 0);
    runtimeEdgeIdToPendingConnections.computeIfAbsent(runtimeEdgeId, id -> new ArrayDeque<>());
    final int currentOutstandingConnections = runtimeEdgeIdToNumCurrentConnections.get(runtimeEdgeId);

    if (currentOutstandingConnections < maxNum) {
      // grant immediately
      runtimeEdgeIdToNumCurrentConnections.put(runtimeEdgeId, currentOutstandingConnections + 1);
      return CompletableFuture.completedFuture(null);
    } else {
      // add to pending queue
      final CompletableFuture<Void> future = new CompletableFuture<>();
      runtimeEdgeIdToPendingConnections.get(runtimeEdgeId).add(future);
      return future;
    }
  }

  /**
   * Indicates the connection has finished.
   * @param runtimeEdgeId the corresponding runtime edge id.
   */
  public synchronized void onConnectionFinished(final String runtimeEdgeId) {
    final Queue<CompletableFuture<Void>> pendingConnections = runtimeEdgeIdToPendingConnections.get(runtimeEdgeId);
    if (pendingConnections.size() == 0) {
      // Just decrease the number of current connections.
      // Since we have no pending connections, we leave pendingConnections queue untouched.
      final int numCurrentConnections = runtimeEdgeIdToNumCurrentConnections.get(runtimeEdgeId);
      runtimeEdgeIdToNumCurrentConnections.put(runtimeEdgeId, numCurrentConnections - 1);
    } else {
      // Since pendingConnections has at least one element, the poll method invocation will immediately return.
      // One connection is completed, and another connection kicks in; the number of current connection stays same
      final CompletableFuture<Void> nextFuture = pendingConnections.poll();
      nextFuture.complete(null);
    }
  }
}
