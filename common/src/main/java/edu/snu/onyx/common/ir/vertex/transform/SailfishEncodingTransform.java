/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.onyx.common.ir.vertex.transform;

import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.common.ir.OutputCollector;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * A {@link Transform} encodes input values into bytes and emits.
 * Through this transform, the {@link RelayTransform} can receive data in a form of byte array in Sailfish optimization.
 * @param <T> input type.
 */
public final class SailfishEncodingTransform<T> implements Transform<T, byte[]> {
  private OutputCollector<byte[]> outputCollector;
  private final Coder<T> coder;

  /**
   * Default constructor.
   * @param coder coder for encoding.
   */
  public SailfishEncodingTransform(final Coder<T> coder) {
    this.coder = coder;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<byte[]> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final Object element) {
      try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
        coder.encode((T) element, outputStream);
        outputCollector.emit(outputStream.toByteArray());
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
  }

  @Override
  public void close() {
    // Do nothing.
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(SailfishEncodingTransform.class);
    sb.append(":");
    sb.append(super.toString());
    return sb.toString();
  }
}
