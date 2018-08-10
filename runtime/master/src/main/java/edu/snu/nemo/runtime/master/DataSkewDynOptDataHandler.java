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
package edu.snu.nemo.runtime.master;

import edu.snu.nemo.runtime.common.comm.ControlMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handler for aggregating data used in data skew dynamic optimization.
 */
public class DataSkewDynOptDataHandler implements DynOptDataHandler {
  private final Map<Integer, Long> aggregatedDynOptData;

  public DataSkewDynOptDataHandler() {
    this.aggregatedDynOptData = new HashMap<>();
  }

  /**
   * Updates data for dynamic optimization sent from Tasks.
   * @param dynOptData data used for data skew dynamic optimization.
   */
  @Override
  public final void updateDynOptData(final Object dynOptData) {
    List<ControlMessage.PartitionSizeEntry> partitionSizeInfo
        = (List<ControlMessage.PartitionSizeEntry>) dynOptData;
    partitionSizeInfo.forEach(partitionSizeEntry -> {
      final int hashIndex = partitionSizeEntry.getKey();
      final long partitionSize = partitionSizeEntry.getSize();
      if (aggregatedDynOptData.containsKey(hashIndex)) {
        aggregatedDynOptData.compute(hashIndex, (originalKey, originalValue) -> originalValue + partitionSize);
      } else {
        aggregatedDynOptData.put(hashIndex, partitionSize);
      }
    });
  }

  /**
   * Returns aggregated data for dynamic optimization.
   * @return aggregated data used for data skew dynamic optimization.
   */
  @Override
  public final Object getDynOptData() {
    return aggregatedDynOptData;
  }
}
