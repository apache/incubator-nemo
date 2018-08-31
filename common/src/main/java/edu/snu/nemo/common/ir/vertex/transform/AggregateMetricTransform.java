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

import edu.snu.nemo.common.Pair;
import edu.snu.nemo.common.ir.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * A {@link Transform} relays input data from upstream vertex to downstream vertex promptly.
 * This transform can be used for merging input data into the {@link OutputCollector}.
 * @param <I> input type.
 * @param <O> output type.
 */
public final class AggregateMetricTransform<I, O> implements Transform<I, O> {
  private static final Logger LOG = LoggerFactory.getLogger(AggregateMetricTransform.class.getName());
  private OutputCollector<O> outputCollector;
  private O aggregatedDynOptData;

  /**
   * Default constructor.
   */
  public AggregateMetricTransform(final O aggregatedDynOptData) {
    this.aggregatedDynOptData = aggregatedDynOptData;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<O> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final I element) {
    // Aggregate key frequency data.
    Object key = ((Pair<Object, Long>) element).left();
    Object count = ((Pair<Object, Long>) element).right();

    Map<Object, Long> aggregatedDynOptDataMap = (Map<Object, Long>) aggregatedDynOptData;
    if (aggregatedDynOptDataMap.containsKey(key)) {
      aggregatedDynOptDataMap.compute(key, (existingKey, accumulatedCount) -> accumulatedCount + (long) count);
    } else {
      aggregatedDynOptDataMap.put(key, (long) count);
    }
  }

  @Override
  public void close() {
    Map<Object, Long> aggregatedDynOptDataMap = (Map<Object, Long>) aggregatedDynOptData;
    outputCollector.emit(aggregatedDynOptData);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(AggregateMetricTransform.class);
    sb.append(":");
    sb.append(super.toString());
    return sb.toString();
  }
}
