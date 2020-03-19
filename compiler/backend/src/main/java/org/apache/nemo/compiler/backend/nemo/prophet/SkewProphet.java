package org.apache.nemo.compiler.backend.nemo.prophet;

import org.apache.nemo.runtime.common.comm.ControlMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class SkewProphet implements Prophet{
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
