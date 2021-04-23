package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.punctuation.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public interface R2WatermarkManager {
  void addDataFetcher(String vmEdgeId, String lambdaEdgeId, int parallelism);
  boolean stopIndex(final int taskIndex,
                    final String edgeId);

  void startIndex(final int taskIndex,
                         final String edgeId);

  void startAndStopPairIndex(final int taskIndex,
                         final String edgeId);

  Optional<Watermark> updateWatermark(final String edgeId,
                                             final int taskIndex, final long watermark);
}
