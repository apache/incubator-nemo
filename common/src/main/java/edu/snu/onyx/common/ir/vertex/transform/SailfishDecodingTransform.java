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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;

/**
 * A {@link Transform} decodes input values into bytes and emits.
 * Through this transform, the {@link RelayTransform} can emit data in a form of byte array in Sailfish optimization.
 * @param <T> output type.
 */
public final class SailfishDecodingTransform<T> implements Transform<byte[], T> {
  private OutputCollector<T> outputCollector;
  private final Coder<T> coder;

  /**
   * Default constructor.
   * @param coder coder for decoding.
   */
  public SailfishDecodingTransform(final Coder<T> coder) {
    this.coder = coder;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<T> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final Object element) {
      try (final ByteArrayInputStream inputStream = new ByteArrayInputStream((byte[])element)) {
        outputCollector.emit(coder.decode(inputStream));
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
  }

  @Override
  public void close(boolean trigger) {
    // Do nothing.
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(SailfishDecodingTransform.class);
    sb.append(":");
    sb.append(super.toString());
    return sb.toString();
  }
}
