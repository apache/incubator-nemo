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
package org.apache.nemo.common.punctuation;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Serializable;
import java.util.Objects;

/**
 * This contains a watermark and the src task index.
 * It is used for transferring the watermark between tasks.
 */
public final class TransientFinishMark implements Serializable {
  private final int index;

  public void encode(final DataOutputStream dos) {
     try {
       dos.writeInt(index);
     } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public static TransientFinishMark decode(final DataInputStream is) {
    try {
      final int index = is.readInt();
      return new TransientFinishMark(index);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public TransientFinishMark(final int index) {
    this.index = index;
  }

  public int getIndex() {
    return index;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("TransientFinishMark from ");
    sb.append(index);
    return sb.toString();
  }
}
