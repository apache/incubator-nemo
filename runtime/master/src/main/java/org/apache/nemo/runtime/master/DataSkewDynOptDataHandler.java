package org.apache.nemo.runtime.master;

import org.apache.nemo.runtime.common.comm.ControlMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handler for aggregating data used in data skew dynamic optimization.
 */
public class DataSkewDynOptDataHandler implements DynOptDataHandler {
  private final Map<Object, Long> aggregatedDynOptData;

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
      final Object key = partitionSizeEntry.getKey();
      final long partitionSize = partitionSizeEntry.getSize();
      if (aggregatedDynOptData.containsKey(key)) {
        aggregatedDynOptData.compute(key, (originalKey, originalValue) -> originalValue + partitionSize);
      } else {
        aggregatedDynOptData.put(key, partitionSize);
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
