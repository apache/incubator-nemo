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
package edu.snu.vortex.runtime.master;

import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.compiler.ir.OutputCollector;

import java.util.ArrayList;
import java.util.List;

/**
 * Output Collector Implementation.
 */
public final class OutputCollectorImpl implements OutputCollector {
  private final List<Element> outputList;

  public OutputCollectorImpl() {
    outputList = new ArrayList<>();
  }

  @Override
  public void emit(final Element output) {
    outputList.add(output);
  }

  @Override
  public void emit(final String dstVertexId, final Element output) {
    throw new UnsupportedOperationException("emit(dstVertexId, output) in OutputCollectorImpl.");
  }

  public Iterable<Element> getOutputList() {
    return outputList;
  }
}
