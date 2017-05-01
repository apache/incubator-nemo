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
package edu.snu.vortex.runtime.executor.channel_to_deprecate;


import edu.snu.vortex.compiler.ir.Element;

/**
 * Interface of channel selectors that determine which channels a given record should be written into.
 */
public interface ChannelSelector {
  /**
   * Returns the channel indexes, to which the given record should be written.
   *
   * @param record        the record to determine which partitions it is written into.
   * @return a (possibly empty) array of integer numbers which indicate the indices of the channels to
   * which the record shall be written.
   */
  int[] selectChannels(Element record);
}