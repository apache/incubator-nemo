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
package edu.snu.vortex.runtime.common.plan.physical;

import edu.snu.vortex.compiler.ir.Reader;

/**
 * BoundedSourceTask.
 * @param <O> the output type.
 */
public final class BoundedSourceTask<O> extends Task {
  private final Reader<O> reader;
  public BoundedSourceTask(final String taskId,
                           final String runtimeVertexId,
                           final int index,
                           final Reader<O> reader) {
    super(taskId, runtimeVertexId, index);
    this.reader = reader;
  }

  public Reader getReader() {
    return reader;
  }
}
