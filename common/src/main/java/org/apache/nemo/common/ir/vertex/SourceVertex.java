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
package org.apache.nemo.common.ir.vertex;

import org.apache.nemo.common.ir.Readable;

import java.util.List;

/**
 * IRVertex that reads data from an external source.
 * It is to be implemented in the compiler frontend with source-specific data fetching logic.
 * @param <O> output type.
 */
public abstract class SourceVertex<O> extends IRVertex {

  /**
   * Constructor for SourceVertex.
   */
  public SourceVertex() {
    super();
  }

  /**
   * @return true if it is bounded source
   */
  public abstract boolean isBounded();

  /**
   * Copy Constructor for SourceVertex.
   *
   * @param that the source object for copying
   */
  public SourceVertex(final SourceVertex that) {
    super(that);
  }
  /**
   * Gets parallel readables.
   *
   * @param desiredNumOfSplits number of splits desired.
   * @return the list of readables.
   * @throws Exception if fail to get.
   */
  public abstract List<Readable<O>> getReadables(int desiredNumOfSplits) throws Exception;

  /**
   * Clears internal states, must be called after getReadables().
   * Concretely, this clears the huge list of input splits held by objects like BeamBoundedSourceVertex before
   * sending the vertex to remote executors.
   * Between clearing states of an existing vertex, and creating a new vertex, we've chosen the former approach
   * to ensure consistent use of the same IRVertex object across the compiler, the master, and the executors.
   */
  public abstract void clearInternalStates();
}
