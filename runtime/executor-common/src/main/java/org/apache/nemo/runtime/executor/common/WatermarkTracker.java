package org.apache.nemo.runtime.executor.common;

import java.util.Optional;

public interface WatermarkTracker {
  Optional<Long> trackAndEmitWatermarks(String taskId,
                                        String edgeId,
                                        final int edgeIndex,
                                        final long watermark);
}
