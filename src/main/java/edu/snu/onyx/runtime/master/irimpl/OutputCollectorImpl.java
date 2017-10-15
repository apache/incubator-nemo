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
package edu.snu.onyx.runtime.master.irimpl;

import edu.snu.onyx.compiler.ir.Element;
import edu.snu.onyx.compiler.ir.OutputCollector;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Output Collector Implementation.
 */
public final class OutputCollectorImpl implements OutputCollector {
  private AtomicReference<List<Element>> outputList;

  /**
   * Constructor of a new OutputCollector.
   */
  public OutputCollectorImpl() {
    outputList = new AtomicReference<>(new ArrayList<>());
  }

  @Override
  public void emit(final Element output) {
    outputList.get().add(output);
  }

  @Override
  public void emit(final String dstVertexId, final Element output) {
    throw new UnsupportedOperationException("emit(dstVertexId, output) in OutputCollectorImpl.");
  }

  /**
   * Collects the accumulated output and replace the output list.
   *
   * @return the list of output elements.
   */
  public List<Element> collectOutputList() {
    return outputList.getAndSet(new ArrayList<>());
  }
}
