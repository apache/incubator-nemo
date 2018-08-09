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
package edu.snu.nemo.runtime.executor.data.streamchainer;

import edu.snu.nemo.common.exception.UnsupportedCompressionException;
import edu.snu.nemo.common.ir.edge.executionproperty.CompressionProperty;
import net.jpountz.lz4.LZ4BlockInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * {@link DecodeStreamChainer} for applying compression.
 */
public class DecompressionStreamChainer implements DecodeStreamChainer {
  private final CompressionProperty.Value compression;

  /**
   * Constructor.
   *
   * @param compression compression method.
   */
  public DecompressionStreamChainer(final CompressionProperty.Value compression) {
    this.compression = compression;
  }

  @Override
  public final InputStream chainInput(final InputStream in) throws IOException {
    switch (compression) {
      case Gzip:
        return new GZIPInputStream(in);
      case LZ4:
        return new LZ4BlockInputStream(in);
      case None:
        return in;
      default:
        throw new UnsupportedCompressionException("Not supported compression method");
    }
  }
}
