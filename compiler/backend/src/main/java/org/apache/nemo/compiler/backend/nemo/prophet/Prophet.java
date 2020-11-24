/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.compiler.backend.nemo.prophet;

import java.util.Map;

/**
 * A prophet class for dynamic optimization.
 * It calculates the statistics the runtime pass needs by (including, but not limited to) parsing the
 * messages accumulated by the RuntimeMessageOutputCollector, and feeds the information to the runtime pass
 * in NemoPlanRewriter.
 * @param <K> key type of the optimized value
 * @param <V> type of optimized value
 */
public interface Prophet<K, V> {
  /**
   * Calculate the optimized values based on runtime messages.
   * @return A map containing the optimized values.
   */
  Map<K, V> calculate();
}
