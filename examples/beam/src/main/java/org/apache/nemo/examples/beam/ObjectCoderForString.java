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

package org.apache.nemo.examples.beam;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * This coder acts like an object coder to conform the type, but is actually just a {@link StringUtf8Coder}.
 * This should only be used in a context where the actual type is a {@link String}.
 * Used in {@link EDGARTop10BadRefererDocs}.
 */
public final class ObjectCoderForString extends AtomicCoder<Object> {
  /**
   * Public accessor of the coder.
   * @return the coder.
   */
  public static ObjectCoderForString of() {
    return INSTANCE;
  }

  /**
   * Instance of the object coder.
   */
  private static final ObjectCoderForString INSTANCE = new ObjectCoderForString();

  /**
   * The actual coder.
   */
  private final StringUtf8Coder coder;

  /**
   * Private constructor.
   */
  private ObjectCoderForString() {
    this.coder = StringUtf8Coder.of();
  }

  @Override
  public void encode(final Object value, final OutputStream outStream) throws IOException {
    coder.encode((String) value, outStream);
  }

  @Override
  public Object decode(final InputStream inStream) throws IOException {
    return coder.decode(inStream);
  }
}
