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

import java.util.Map;

/**
 * Do operator.
 * @param <I> input type.
 * @param <O> output type.
 * @param <T> .
 */
public abstract class Do<I, O, T> extends Operator<I, O> {
  // We assume for now that broadcasted data are only used in Do
  public abstract Iterable<O> transform(Iterable<I> input, Map<T, Object> broadcasted);
}
