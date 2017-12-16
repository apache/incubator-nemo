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
package edu.snu.onyx.runtime.common.plan.physical;

import edu.snu.onyx.common.ir.Reader;
import org.apache.beam.sdk.io.BoundedSource;

/**
 * SourceTask.
 * @param <O> the output type.
 */
public final class SourceTask<O> extends Task {
  private final Reader<O> reader;
  private final boolean isBoundedSource;

  /**
   * Constructor.
   * @param taskId id of the task.
   * @param runtimeVertexId id of the runtime vertex.
   * @param index index in its taskGroup.
   * @param reader reader for the source data.
   * @param taskGroupId id of the taskGroup.
   */
  public SourceTask(final String taskId,
                    final String runtimeVertexId,
                    final int index,
                    final Reader<O> reader,
                    final String taskGroupId) {
    super(taskId, runtimeVertexId, index, taskGroupId);
    this.reader = reader;
    this.isBoundedSource = (reader instanceof BoundedSource.Reader);
  }

  /**
   * @return the boolean value which tells source reader is Bounded or not.
   */
  public boolean isBoundedSource() {
    return this.isBoundedSource;
  }

  /**
   * @return the reader of source data.
   */
  public Reader getReader() {
    return reader;
  }
}
