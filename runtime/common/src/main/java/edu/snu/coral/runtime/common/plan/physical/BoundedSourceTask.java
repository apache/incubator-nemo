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
package edu.snu.coral.runtime.common.plan.physical;

import edu.snu.coral.common.ir.Readable;
import edu.snu.coral.common.ir.ReadablesWrapper;

/**
 * BoundedSourceTask.
 * @param <O> the output type.
 */
public final class BoundedSourceTask<O> extends Task {
  private final ReadablesWrapper<O> readableWrapper;

  /**
   * Constructor.
   * @param taskId id of the task.
   * @param irVertexId id of the IR vertex.
   * @param readablesWrapper the wrapper of the readables for the source data.
   */
  public BoundedSourceTask(final String taskId,
                           final String irVertexId,
                           final ReadablesWrapper<O> readablesWrapper) {
    super(taskId, irVertexId);
    this.readableWrapper = readablesWrapper;
  }

  /**
   * @param readableIdx the index of the target readable.
   * @return the readable of source data.
   * @throws Exception if fail to get.
   */
  public Readable<O> getReadable(final int readableIdx) throws Exception {
    return readableWrapper.getReadables().get(readableIdx);
  }
}
