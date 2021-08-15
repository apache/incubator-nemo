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
package org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.exception.SchedulingException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ShuffleExecutorSetProperty;
import org.apache.nemo.compiler.frontend.beam.transform.CombineTransform;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Pass for inserting intermediate aggregator for partial shuffle.
 */
@Requires(ParallelismProperty.class)
public class IntermediateAccumulatorInsertionPass extends ReshapingPass {
  private final String networkFilePath;
  private boolean isUnitTest = false;
  private static final Map<String, ArrayList<String>> UNIT_TEST_NETWORK_FILE = getUnitTestNetworkFile();

  /**
   * Default constructor.
   */
  public IntermediateAccumulatorInsertionPass() {
    super(IntermediateAccumulatorInsertionPass.class);
    this.networkFilePath = Util.fetchProjectRootPath() + "/bin/labeldict.json";
  }

  /**
   * Constructor for unit test.
   * @param isUnitTest indicates unit test.
   */
  public IntermediateAccumulatorInsertionPass(final boolean isUnitTest) {
    this();
    this.isUnitTest = isUnitTest;
  }

  private static Map<String, ArrayList<String>> getUnitTestNetworkFile() {
    Map<String, ArrayList<String>> map = new HashMap<>();
    map.put("0", new ArrayList<>(Arrays.asList("mulan-16.maas", "0")));
    map.put("1", new ArrayList<>(Arrays.asList("mulan-23.maas", "0")));
    map.put("2", new ArrayList<>(Arrays.asList("mulan-m", "0")));
    map.put("3", new ArrayList<>(Arrays.asList("1+2", "0.00003721")));
    map.put("4", new ArrayList<>(Arrays.asList("0+3", "2.19395143")));
    return map;
  }

  /**
   * Insert accumulator vertex based on network hierarchy.
   *
   * @param irdag irdag to apply pass.
   * @return modified irdag.
   */
  @Override
  public IRDAG apply(final IRDAG irdag) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      Map<String, ArrayList<String>> map;
      if (isUnitTest) {
        map = UNIT_TEST_NETWORK_FILE;
      } else {
        map = mapper.readValue(new File(networkFilePath), Map.class);
      }

      irdag.topologicalDo(v -> {
        if (v instanceof OperatorVertex && ((OperatorVertex) v).getTransform() instanceof CombineTransform) {
          final CombineTransform finalCombineStreamTransform = (CombineTransform) ((OperatorVertex) v).getTransform();
          if (finalCombineStreamTransform.getIntermediateCombine().isPresent()) {
            irdag.getIncomingEdgesOf(v).forEach(e -> {
              if (CommunicationPatternProperty.Value.SHUFFLE
                .equals(e.getPropertyValue(CommunicationPatternProperty.class)
                  .orElse(CommunicationPatternProperty.Value.ONE_TO_ONE))) {
                handleDataTransferFor(irdag, map, finalCombineStreamTransform, e, 10F);
              }
            });
          }
        }
      });

      return irdag;
    } catch (final Exception e) {
      throw new SchedulingException(e);
    }
  }

  private static void handleDataTransferFor(final IRDAG irdag,
                                            final Map<String, ArrayList<String>> map,
                                            final CombineTransform finalCombineStreamTransform,
                                            final IREdge targetEdge,
                                            final Float threshold) {
    final int srcParallelism = targetEdge.getSrc().getPropertyValue(ParallelismProperty.class).get();

    final int mapSize = map.size();
    final int numOfNodes = (mapSize + 1) / 2;
    Float previousDistance = 0F;

    for (int i = numOfNodes; i < mapSize; i++) {
      final float currentDistance = Float.parseFloat(map.get(String.valueOf(i)).get(1));
      if (previousDistance != 0 && currentDistance > threshold * previousDistance
        && srcParallelism * 2 / 3 >= mapSize - i + 1) {
        final Integer targetNumberOfSets = mapSize - i;
        final HashSet<HashSet<String>> setsOfExecutors = getTargetNumberOfExecutorSetsFrom(map, targetNumberOfSets);

        final CombineTransform<?, ?, ?> intermediateCombineStreamTransform =
          (CombineTransform) finalCombineStreamTransform.getIntermediateCombine().get();
        final OperatorVertex accumulatorVertex = new OperatorVertex(intermediateCombineStreamTransform);

        targetEdge.getDst().copyExecutionPropertiesTo(accumulatorVertex);
        accumulatorVertex.setProperty(ParallelismProperty.of(srcParallelism * 2 / 3));
        accumulatorVertex.setProperty(ShuffleExecutorSetProperty.of(setsOfExecutors));

        irdag.insert(accumulatorVertex, targetEdge);
        break;
      }
      previousDistance = currentDistance;
    }
  }

  private static HashSet<HashSet<String>> getTargetNumberOfExecutorSetsFrom(final Map<String, ArrayList<String>> map,
                                                                            final Integer targetNumber) {
    final HashSet<HashSet<String>> result = new HashSet<>();
    final Integer index = map.size() - targetNumber;
    final List<String> indicesToCheck = IntStream.range(0, index)
      .map(i -> -i).sorted().map(i -> -i)
      .mapToObj(String::valueOf)
      .collect(Collectors.toList());

    Arrays.asList(map.get(String.valueOf(index)).get(0).split("\\+"))
      .forEach(key -> result.add(recursivelyExtractExecutorsFrom(map, key, indicesToCheck)));

    while (!indicesToCheck.isEmpty()) {
      result.add(recursivelyExtractExecutorsFrom(map, indicesToCheck.get(0), indicesToCheck));
    }

    return result;
  }

  private static HashSet<String> recursivelyExtractExecutorsFrom(final Map<String, ArrayList<String>> map,
                                                                 final String key,
                                                                 final List<String> indicesToCheck) {
    indicesToCheck.remove(key);
    final HashSet<String> result = new HashSet<>();
    final List<String> indices = Arrays.asList(map.get(key).get(0).split("\\+"));
    if (indices.size() == 1) {
      result.add(indices.get(0));
    } else {
      indices.forEach(index -> result.addAll(recursivelyExtractExecutorsFrom(map, index, indicesToCheck)));
    }
    return result;
  }
}
