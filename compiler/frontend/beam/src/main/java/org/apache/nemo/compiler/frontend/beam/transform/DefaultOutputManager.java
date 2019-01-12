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
package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.nemo.common.ir.OutputCollector;

/**
 * Default output emitter that uses outputCollector.
 * @param <OutputT> output type
 */
public final class DefaultOutputManager<OutputT> implements DoFnRunners.OutputManager {
  private final TupleTag<OutputT> mainOutputTag;
  private final OutputCollector<WindowedValue<OutputT>> outputCollector;

  /**
   * @param outputCollector output collector.
   * @param mainOutputTag main output tag.
   */
  DefaultOutputManager(final OutputCollector<WindowedValue<OutputT>> outputCollector,
                       final TupleTag<OutputT> mainOutputTag) {
    this.outputCollector = outputCollector;
    this.mainOutputTag = mainOutputTag;
  }

  @Override
  public <T> void output(final TupleTag<T> tag, final WindowedValue<T> output) {
    if (tag.equals(mainOutputTag)) {
      outputCollector.emit((WindowedValue<OutputT>) output);
    } else {
      outputCollector.emit(tag.getId(), output);
    }
  }
}
