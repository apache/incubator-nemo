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

import edu.snu.nemo.common.ir.vertex.transform.Transform;

/**
 * OperatorTask.
 */
public final class OperatorTask extends Task {
  private final Transform transform;

  /**
   * Constructor.
   * @param taskId id of the task.
   * @param runtimeVertexId id of the runtime vertex.
   * @param transform transform to perform.
   */
  public OperatorTask(final String taskId,
                      final String runtimeVertexId,
                      final Transform transform) {
    super(taskId, runtimeVertexId);
    this.transform = transform;
  }

  /**
   * @return the transform to perform.
   */
  public Transform getTransform() {
    return transform;
  }
}
