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
package edu.snu.vortex.compiler.frontend.beam.transform;

import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.compiler.ir.OutputCollector;
import edu.snu.vortex.compiler.ir.Transform;

import java.util.ArrayList;

/**
 * Flatten transform implementation.
 */
public final class FlattenTransform implements Transform {
  private final ArrayList<Element> collectedElements;
  private OutputCollector outputCollector;

  /**
   * FlattenTransform Constructor.
   */
  public FlattenTransform() {
    this.collectedElements = new ArrayList<>();
  }

  @Override
  public void prepare(final Context context, final OutputCollector oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final Iterable<Element> data, final String srcVertexId) {
    data.forEach(collectedElements::add);
  }

  @Override
  public void close() {
    collectedElements.forEach(outputCollector::emit);
    collectedElements.clear();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("FlattenTransform:");
    sb.append(super.toString());
    return sb.toString();
  }
}
