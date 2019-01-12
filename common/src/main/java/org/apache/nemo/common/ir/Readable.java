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
package org.apache.nemo.common.ir;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Interface for readable.
 * @param <O> output type.
 */
public interface Readable<O> extends Serializable {

  /**
   * Prepare reading data.
   */
  void prepare();

  /**
   * Method to read current data from the source.
   * The caller should check whether the Readable is finished or not by using isFinished() method
   * before calling this method.
   *
   * It can throw NoSuchElementException although it is not finished in Unbounded source.
   * @return a data read by the readable.
   * @throws NoSuchElementException when no element exists
   */
  O readCurrent() throws NoSuchElementException;

  /**
   * Read watermark.
   * @return watermark
   */
  long readWatermark();

  /**
   * @return true if it reads all data.
   */
  boolean isFinished();

  /**
   * Returns the list of locations where this readable resides.
   * Each location has a complete copy of the readable.
   *
   * @return List of locations where this readable resides
   * @throws UnsupportedOperationException when this operation is not supported
   * @throws Exception                     any other exceptions on the way
   */
  List<String> getLocations() throws Exception;

  /**
   * Close.
   * @throws IOException if file-based reader throws any.
   */
  void close() throws IOException;
}
