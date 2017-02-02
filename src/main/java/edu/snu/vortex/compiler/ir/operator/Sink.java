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
package edu.snu.vortex.compiler.ir.operator;

import java.util.List;

/**
 * Sink operator.
 * @param <I> input type.
 */
public abstract class Sink<I> extends Operator<I, Void> {
  // Maybe make the parameter a any-type hashmap(attributes/options)

  /**
   * Getter for writers.
   * @param numWriters .
   * @return List of writers.
   * @throws Exception .
   */
  public abstract List<Writer<I>> getWriters(final int numWriters) throws Exception;

  /**
   * Interface for writer.
   * @param <I> input type.
   */
  interface Writer<I> {
    void write(Iterable<I> data) throws Exception;
  }
}
