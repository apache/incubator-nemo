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
package edu.snu.vortex.runtime.executor.channel;


import edu.snu.vortex.compiler.ir.Element;

import javax.annotation.Nullable;


/**
 * Input channel interface.
 */
public interface InputChannel extends Channel {

  /**
   * read data transferred from the respective {@link OutputChannel}.
   * if no data available, it immediately returns with null.
   * @return an iterable of data elements.
   */
  @Nullable
  Iterable<Element> read();

  /**
   * read data transferred from the respective {@link OutputChannel}.
   * if there is no data to read, it will be blocked until data get available.
   * @param timeout the timeout in millisecond.
   *                if the value is the maximum value of {@link Long}, it wait without timeout.
   * @return an iterable of data elements.
   */
  Iterable<Element> read(long timeout);
}
