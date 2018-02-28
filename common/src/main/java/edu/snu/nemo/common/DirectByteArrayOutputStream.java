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
package edu.snu.nemo.common;

import java.io.ByteArrayOutputStream;

/**
 * This class represents a custom implementation of {@link ByteArrayOutputStream},
 * which enables to get bytes buffer directly (without memory copy).
 */
public final class DirectByteArrayOutputStream extends ByteArrayOutputStream {

  /**
   * Default constructor.
   */
  public DirectByteArrayOutputStream() {
    super();
  }

  /**
   * Constructor specifying the size.
   * @param size the initial size.
   */
  public DirectByteArrayOutputStream(final int size) {
    super(size);
  }

  /**
   * @return the buffer where data is stored.
   */
  public byte[] getBufDirectly() {
    return buf;
  }

  /**
   * @return the number of valid bytes in the buffer.
   */
  public int getCount() {
    return count;
  }
}
