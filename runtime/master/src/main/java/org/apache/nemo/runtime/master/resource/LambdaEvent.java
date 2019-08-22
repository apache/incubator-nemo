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
package org.apache.nemo.runtime.master.resource;

import io.netty.buffer.ByteBuf;

import java.io.Serializable;

/**
 * LambdaEvent contains various types for LambdaExecutor to deal with.
 */
public final class LambdaEvent implements Serializable {

  /**
   * Types supported in LambdaEvent, explaining what events will be transmitted to and from LambdaExecutor.
   * TODO #407: LambdaHandler for single-stage execution
   * Currently not all of those types are used.
   */
  public enum Type {
    VM_RUN,
    CLIENT_HANDSHAKE,
    WORKER_INIT,
    WORKER_INIT_DONE,
    STREAM_WORKER_INIT,
    DATA,
    GBK_START,
    GBK,
    RESULT,
    WARMUP_END,
    END,
    CPU_LOAD
  }

  private final Type type;
  private final byte[] bytes;
  private final int len;

  public LambdaEvent(final Type type,
                         final byte[] bytes,
                         final int len) {
    this.type = type;
    this.bytes = bytes;
    this.len = len;
  }

  public Type getType() {
    return type;
  }

  public byte[] getBytes() {
    return bytes;
  }

  public int getLen() {
    return len;
  }

  @Override
  public String toString() {
    return "LambdaEvent:" + type.name();
  }
}

