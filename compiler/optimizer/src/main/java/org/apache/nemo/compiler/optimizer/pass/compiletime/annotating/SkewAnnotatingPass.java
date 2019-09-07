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
package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.PartitionerProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.utility.MessageAggregatorVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For each shuffle edge, set the number of partitions to (dstParallelism * HASH_RANGE_MULTIPLIER).
 * With this finer-grained partitioning, we can dynamically assign partitions to destination tasks based on data sizes.
 */
@Annotates(PartitionerProperty.class)
@Requires(CommunicationPatternProperty.class)
public final class SkewAnnotatingPass extends AnnotatingPass {
  private static final Logger LOG = LoggerFactory.getLogger(SkewAnnotatingPass.class.getName());

  /**
   * Hash range multiplier.
   * If we need to split or recombine an output data from a task after it is stored,
   * we multiply the hash range with this factor in advance
   * to prevent the extra deserialize - rehash - serialize process.
   * In these cases, the hash range will be (hash range multiplier X destination task parallelism).
   */
  public static final int HASH_RANGE_MULTIPLIER = 5;

  /**
   * Default constructor.
   */
  public SkewAnnotatingPass() {
    super(SkewAnnotatingPass.class);
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    dag.topologicalDo(v -> {
      dag.getIncomingEdgesOf(v).forEach(e -> {
        if (CommunicationPatternProperty.Value.SHUFFLE
          .equals(e.getPropertyValue(CommunicationPatternProperty.class).get())
          && !(e.getDst() instanceof MessageAggregatorVertex)) {


          // Set the partitioner property
          final int dstParallelism = e.getDst().getPropertyValue(ParallelismProperty.class).get();
          LOG.info("SET {} to {} * {}", e.getId(), dstParallelism, HASH_RANGE_MULTIPLIER);
          e.setPropertyPermanently(PartitionerProperty.of(
            PartitionerProperty.Type.HASH, dstParallelism * HASH_RANGE_MULTIPLIER));
        }
      });
    });
    return dag;
  }
}

