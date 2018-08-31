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
package edu.snu.nemo.common.ir.vertex.transform;

import edu.snu.nemo.common.KeyExtractor;
import edu.snu.nemo.common.Pair;
import edu.snu.nemo.common.ir.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * A {@link Transform} relays input data from upstream vertex to downstream vertex promptly.
 * This transform can be used for merging input data into the {@link OutputCollector}.
 * @param <T> input/output type.
 */
public final class MetricCollectTransform<T> implements Transform<T, T> {
  private static final Logger LOG = LoggerFactory.getLogger(MetricCollectTransform.class.getName());
  private OutputCollector<T> outputCollector;
  private final String dstvertexId;
  private final KeyExtractor keyExtractor;
  private Map<Object, Long> dynOptData;

  /**
   * Default constructor.
   */
  public MetricCollectTransform(final String dstVertexId, final KeyExtractor keyExtractor) {
    this.dynOptData = new HashMap<>();
    this.dstvertexId = dstVertexId;
    this.keyExtractor = keyExtractor;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<T> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final T element) {
    // Collect key frequency data, which is used in dynamic optimization.
    Object key = keyExtractor.extractKey(element);
    if (dynOptData.containsKey(key)) {
      dynOptData.compute(key, (existingKey, existingCount) -> existingCount + 1L);
    } else {
      dynOptData.put(key, 1L);
    }
  }

  @Override
  public void close() {
    dynOptData.forEach((k, v) -> {
      final Pair<Object, Long> pairData = Pair.of(k, v);
      outputCollector.emit(dstvertexId, pairData);
    });
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(MetricCollectTransform.class);
    sb.append(":");
    sb.append(super.toString());
    return sb.toString();
  }
}
