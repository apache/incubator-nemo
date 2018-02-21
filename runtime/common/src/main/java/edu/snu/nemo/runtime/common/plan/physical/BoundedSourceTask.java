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
package edu.snu.nemo.runtime.common.plan.physical;

import edu.snu.nemo.common.ir.Readable;

/**
 * BoundedSourceTask.
 * @param <O> the output type.
 */
public final class BoundedSourceTask<O> extends Task {
  private Readable<O> readable;

  /**
   * Constructor.
   * @param taskId id of the task.
   * @param irVertexId id of the IR vertex.
   */
  public BoundedSourceTask(final String taskId,
                           final String irVertexId) {
    super(taskId, irVertexId);
  }

  /**
   * Sets the readable for this task.
   * @param readableToSet the readable to set.
   */
  public void setReadable(final Readable<O> readableToSet) {
    this.readable = readableToSet;
  }

  /**
   * @return the readable of source data.
   */
  public Readable<O> getReadable() {
    return readable;
  }
}
