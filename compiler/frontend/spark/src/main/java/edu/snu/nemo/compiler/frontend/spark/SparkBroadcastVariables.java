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
package edu.snu.nemo.compiler.frontend.spark;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Broadcast variables of Spark.
 */
public final class SparkBroadcastVariables {
  private static final Map<Serializable, Object> TAG_TO_VARIABLE = new HashMap<>();

  private SparkBroadcastVariables() {
  }

  /**
   * @param tag of the broadcast variable.
   * @param variable data.
   */
  public static void put(final Serializable tag, final Object variable) {
    TAG_TO_VARIABLE.put(tag, variable);
  }

  /**
   * @return all the map from tags to variables.
   */
  public static Map<Serializable, Object> getAll() {
    return TAG_TO_VARIABLE;
  }
}
