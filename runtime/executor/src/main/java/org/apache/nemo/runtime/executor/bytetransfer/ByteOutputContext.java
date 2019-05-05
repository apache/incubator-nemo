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
package org.apache.nemo.runtime.executor.bytetransfer;

import io.netty.buffer.ByteBufOutputStream;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.runtime.executor.common.datatransfer.ByteTransferContextSetupMessage;
import org.apache.nemo.runtime.executor.data.DataUtil;
import org.apache.nemo.runtime.executor.data.FileArea;
import org.apache.nemo.runtime.executor.data.partition.SerializedPartition;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Container for multiple output streams. Represents a transfer context on sender-side.
 *
 * <p>Public methods are thread safe,
 * although the execution order may not be linearized if they were called from different threads.</p>
 */
public interface ByteOutputContext extends ByteTransferContext, AutoCloseable {


  ByteOutputStream newOutputStream() throws IOException;

  // pending for moving downstream tasks
  void pending(final boolean scaleout);

  // resume after moving downstream tasks
  void scaleoutToVm(final String address, final String taskId);
  void scaleInToVm();

  void stop();

  void restart();

  void onChannelError(@Nullable final Throwable cause);

  /**
   * An {@link OutputStream} implementation which buffers data to {@link ByteBuf}s.
   *
   * <p>Public methods are thread safe,
   * although the execution order may not be linearized if they were called from different threads.</p>
   */
  public interface ByteOutputStream extends AutoCloseable, Flushable {

    void write(final int i) throws IOException;

    void write(final byte[] bytes, final int offset, final int length) throws IOException;

    ByteOutputStream writeSerializedPartition(final SerializedPartition serializedPartition)  throws IOException;

    /**
     * Writes a data frame from {@link FileArea}.
     *
     * @param fileArea the {@link FileArea} to transfer
     * @return {@code this}
     * @throws IOException when failed to open the file, an exception has been set, or this stream was closed
     */
    ByteOutputStream writeFileArea(final FileArea fileArea) throws IOException;

    void writeElement(final Object element,
                      final Serializer serializer);
  }
}
