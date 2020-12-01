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
package org.apache.nemo.runtime.executor.transfer;

import org.apache.nemo.runtime.executor.data.streamchainer.Serializer;
import java.io.IOException;

/**
 * Represents the output stream to which the sender sends its data during the data transfer.
 */
public interface TransferOutputStream extends AutoCloseable {
  /**
   * Write an element into the output stream.
   * @param element element to be sent
   * @param serializer serializer of {@code element}
   */
  void writeElement(Object element, Serializer serializer);

  /**
   * Closes this output stream.
   * @throws IOException if any exception has occurred. For more information, see {@link ByteOutputContext#close}.
   */
  void close() throws IOException;
}
