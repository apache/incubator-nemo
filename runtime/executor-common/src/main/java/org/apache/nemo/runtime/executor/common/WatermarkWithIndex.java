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
package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.punctuation.Watermark;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Objects;

/**
 * This contains a watermark and the src task index.
 * It is used for transferring the watermark between tasks.
 */
public final class WatermarkWithIndex implements Serializable {
  private final Watermark watermark;
  private final int index;

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    WatermarkWithIndex that = (WatermarkWithIndex) o;
    return index == that.index &&
      Objects.equals(watermark, that.watermark);
  }

  @Override
  public int hashCode() {

    return Objects.hash(watermark, index);
  }

  public void encode(final DataOutputStream dos) {
     try {
       dos.writeInt(index);
       dos.writeLong(watermark.getTimestamp());
     } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public static WatermarkWithIndex decode(final DataInputStream is) {
    try {
      final int index = is.readInt();
      final long wm = is.readLong();
      return new WatermarkWithIndex(new Watermark(wm), index);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public WatermarkWithIndex(final Watermark watermark, final int index) {
    this.watermark = watermark;
    this.index = index;
  }

  public Watermark getWatermark() {
    return watermark;
  }

  public int getIndex() {
    return index;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(watermark);
    sb.append(" from ");
    sb.append(index);
    return sb.toString();
  }
}
