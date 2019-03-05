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

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.nemo.common.ir.AbstractOutputCollector;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.reef.io.Tuple;

import java.util.LinkedList;
import java.util.List;

/**
 * Test output collector that collects data and watermarks.
 * @param <T>
 */
final class TestOutputCollector<T> extends AbstractOutputCollector<WindowedValue<T>> {
  public final List<WindowedValue<T>> outputs;
  public final List<Tuple<String, WindowedValue<T>>> taggedOutputs;
  public final List<Watermark> watermarks;

  TestOutputCollector() {
    this.outputs = new LinkedList<>();
    this.taggedOutputs = new LinkedList<>();
    this.watermarks = new LinkedList<>();
  }

  @Override
  public void emit(WindowedValue<T> output) {
      outputs.add(output);
    }

  @Override
  public void emitWatermark(Watermark watermark) {
    watermarks.add(watermark);
  }


  @Override
  public <O> void emit(String dstVertexId, O output) {
    final WindowedValue<T> val = (WindowedValue<T>) output;
    final Tuple<String, WindowedValue<T>> tuple = new Tuple<>(dstVertexId, val);
    taggedOutputs.add(tuple);
  }

  public List<WindowedValue<T>> getOutput() {
    return outputs;
  }

  public List<Tuple<String, WindowedValue<T>>> getTaggedOutputs() {
      return taggedOutputs;
    }
}
