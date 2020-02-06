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
package org.apache.nemo.runtime.executor.data.streamchainer;

import net.jpountz.lz4.LZ4BlockOutputStream;
import org.apache.nemo.common.exception.UnsupportedCompressionException;
import org.apache.nemo.common.ir.edge.executionproperty.CompressionProperty;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * {@link EncodeStreamChainer} for applying compression.
 */
public class CompressionStreamChainer implements EncodeStreamChainer {
  private final CompressionProperty.Value compression;

  /**
   * Constructor.
   *
   * @param compression compression method.
   */
  public CompressionStreamChainer(final CompressionProperty.Value compression) {
    this.compression = compression;
  }

  @Override
  public final OutputStream chainOutput(final OutputStream out) throws IOException {
    switch (compression) {
      case GZIP:
        return new GZIPOutputStream(out);
      case LZ4:
        return new LZ4BlockOutputStream(out);
      case NONE:
        return out;
      default:
        throw new UnsupportedCompressionException("Not supported compression method");
    }
  }
}
