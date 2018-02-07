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
package edu.snu.nemo.common.ir;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Interface for readable.
 * @param <O> output type.
 */
public interface Readable<O> extends Serializable {
  /**
   * Method to read data from the source.
   * @return an {@link Iterable} of the data read by the readable.
   * @throws Exception exception while reading data.
   */
  Iterable<O> read() throws Exception;

  /**
   * Returns the list of locations where this readable resides.
   * Each location has a complete copy of the readable.
   * @return List of locations where this readable resides, or an empty list if this operation is not supported
   * @throws Exception exceptions on the way
   */
  default List<String> getLocations() throws Exception {
    return Collections.emptyList();
  }
}

