/*
 * Copyright (C) 2018 Seoul National University
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
package org.apache.nemo.runtime.executor;

import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.Stage;

import java.util.ArrayList;
import java.util.List;

public final class TestUtil {
  public static List<String> generateTaskIds(final Stage stage) {
    final List<String> result = new ArrayList<>(stage.getParallelism());
    final int first_attempt = 0;
    for (int taskIndex = 0; taskIndex < stage.getParallelism(); taskIndex++) {
      result.add(RuntimeIdManager.generateTaskId(stage.getId(), taskIndex, first_attempt));
    }
    return result;
  }
}
