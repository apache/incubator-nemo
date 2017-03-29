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
package edu.snu.vortex.compiler.ir;

import java.util.List;

/**
 * Vertex that reads data from an external source.
 * It is to be implemented in the compiler frontend with source-specific data fetching logic.
 * @param <O> output type.
 */
public abstract class SourceVertex<O> extends Vertex {
  /**
   * Get parallel readers.
   * @param desiredNumOfSplits number of splits desired.
   * @return List of readers.
   * @throws Exception .
   */
  public abstract List<Reader<O>> getReaders(final int desiredNumOfSplits) throws Exception;
}
