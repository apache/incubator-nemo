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

import org.apache.nemo.runtime.common.comm.ControlMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Prophet class for skew handling.
 */
public final class SkewProphet implements Prophet {
  private final List<ControlMessage.RunTimePassMessageEntry> messageEntries;
  public SkewProphet(final List<ControlMessage.RunTimePassMessageEntry> messageEntries) {
    this.messageEntries = messageEntries;
  }

  @Override
  public Map<String, Long> calculate() {
    final Map<String, Long> aggregatedData = new HashMap<>();
    messageEntries.forEach(entry -> {
      final String key = entry.getKey();
      final long partitionSize = entry.getValue();
      if (aggregatedData.containsKey(key)) {
        aggregatedData.compute(key, (originalKey, originalValue) -> (long) originalValue + partitionSize);
      } else {
        aggregatedData.put(key, partitionSize);
      }
    });
    return aggregatedData;
  }
}