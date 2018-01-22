/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.onyx.runtime.executor.data;

import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.common.ir.edge.IREdge;
import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.runtime.executor.data.filter.CompressionFilter;
import edu.snu.onyx.runtime.executor.data.filter.Filter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Mapping from RuntimeEdgeId to Coder.
 */
public final class CoderManager {
  private final ConcurrentMap<String, Coder> runtimeEdgeIdToCoder = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, List<Filter>> runtimeEdgeIdToFilter = new ConcurrentHashMap<>();

  /**
   * Constructor.
   */
  @Inject
  public CoderManager() {
  }

  /**
   * Register a coder for runtime edge.
   *
   * @param runtimeEdgeId id of the runtime edge.
   * @param coder         the corresponding coder.
   */
  public void registerCoder(final String runtimeEdgeId, final Coder coder) {
    runtimeEdgeIdToCoder.putIfAbsent(runtimeEdgeId, coder);
  }

  /**
   * Generate filter list and register for a runtime edge.
   *
   * @param edge the runtime edge.
   */
  public void registerFilterList(final IREdge edge) {
    final List<Filter> filterList = new ArrayList<>();

    // Compression filter
    filterList.add(new CompressionFilter(edge.getProperty(ExecutionProperty.Key.Compression)));

    runtimeEdgeIdToFilter.putIfAbsent(edge.getId(), filterList);
  }

  /**
   * Return the coder for the specified runtime edge.
   *
   * @param runtimeEdgeId id of the runtime edge.
   * @return the corresponding coder.
   */
  public Coder getCoder(final String runtimeEdgeId) {
    final Coder coder = runtimeEdgeIdToCoder.get(runtimeEdgeId);
    if (coder == null) {
      throw new RuntimeException("No coder is registered for " + runtimeEdgeId);
    }
    return coder;
  }

  /**
   * Return the list of filters for the specified runtime edge.
   *
   * @param runtimeEdgeId id of the runtime edge.
   * @return the corresponding list of filters.
   */
  public List<Filter> getFilterList(final String runtimeEdgeId) {
    final List<Filter> filters = runtimeEdgeIdToFilter.get(runtimeEdgeId);
    if (filters == null) {
      throw new RuntimeException("No list of filters is registered for " + runtimeEdgeId);
    }
    return filters;
  }
}
