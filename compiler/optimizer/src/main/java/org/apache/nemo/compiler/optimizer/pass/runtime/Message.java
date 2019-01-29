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
package org.apache.nemo.compiler.optimizer.pass.runtime;

import org.apache.nemo.common.ir.edge.IREdge;

import java.util.Set;

/**
 * @param <T> type of the message value.
 */
public class Message<T> {
  private final String messageId;
  private final T value;
  private final Set<IREdge> edgesToExamine;

  public Message(final String messageId, final Set<IREdge> edgesToExamine, final T value) {
    if (messageId == null || edgesToExamine == null || edgesToExamine.isEmpty() || value == null) {
      throw new IllegalArgumentException();
    }
    this.messageId = messageId;
    this.value = value;
    this.edgesToExamine = edgesToExamine;
  }

  public String getMessageId() {
    return messageId;
  }

  public T getMessageValue() {
    return value;
  }

  public Set<IREdge> getExaminedEdges() {
    return edgesToExamine;
  }
}
