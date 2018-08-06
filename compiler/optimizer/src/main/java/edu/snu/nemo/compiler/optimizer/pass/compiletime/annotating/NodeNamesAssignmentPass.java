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
package edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating;

import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.NodeNamesProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.commons.math3.optim.BaseOptimizer;
import org.apache.commons.math3.optim.PointValuePair;
import org.apache.commons.math3.optim.linear.*;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;
import org.apache.commons.math3.util.Incrementor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

/**
 * Computes and assigns appropriate share of nodes to each irVertex to minimize shuffle time,
 * with respect to bandwidth restrictions of nodes. If bandwidth information is not given, this pass does nothing.
 * This pass follows task assignment of Iridium-style optimization.
 * http://pages.cs.wisc.edu/~akella/papers/gda-sigcomm15.pdf
 *
 * <h3>Assumptions</h3>
 * This pass assumes no skew in input or intermediate data, so that the number of Task assigned to a node
 * is proportional to the data size handled by the node.
 * Also, this pass assumes stages with empty map as {@link NodeNamesProperty} are assigned to nodes evenly.
 * For example, if source splits are not distributed evenly, any source location-aware scheduling policy will
 * assign TaskGroups unevenly.
 * Also, this pass assumes network bandwidth to be the bottleneck. Each node should have enough capacity to run
 * TaskGroups immediately as scheduler attempts to schedule a TaskGroup.
 */
public final class NodeNamesAssignmentPass extends AnnotatingPass {

  // Index of the objective parameter, in the coefficient vector
  private static final int OBJECTIVE_COEFFICIENT_INDEX = 0;
  private static final Logger LOG = LoggerFactory.getLogger(NodeNamesAssignmentPass.class);
  private static final HashMap<String, Integer> EMPTY_MAP = new HashMap<>();

  private static String bandwidthSpecificationString = "";


  /**
   * Default constructor.
   */
  public NodeNamesAssignmentPass() {
    super(NodeNamesProperty.class, Collections.singleton(ParallelismProperty.class));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    if (bandwidthSpecificationString.isEmpty()) {
      dag.topologicalDo(irVertex -> irVertex.setProperty(NodeNamesProperty.of(EMPTY_MAP)));
    } else {
      assignNodeShares(dag, BandwidthSpecification.fromJsonString(bandwidthSpecificationString));
    }
    return dag;
  }

  public static void setBandwidthSpecificationString(final String value) {
    bandwidthSpecificationString = value;
  }

  private static HashMap<String, Integer> getEvenShares(final List<String> nodes, final int parallelism) {
    final HashMap<String, Integer> shares = new HashMap<>();
    final int defaultShare = parallelism / nodes.size();
    final int remainder = parallelism % nodes.size();
    for (int i = 0; i < nodes.size(); i++) {
      shares.put(nodes.get(i), defaultShare + (i < remainder ? 1 : 0));
    }
    return shares;
  }

  private static void assignNodeShares(
      final DAG<IRVertex, IREdge> dag,
      final BandwidthSpecification bandwidthSpecification) {
    dag.topologicalDo(irVertex -> {
      final Collection<IREdge> inEdges = dag.getIncomingEdgesOf(irVertex);
      final int parallelism = irVertex.getPropertyValue(ParallelismProperty.class)
          .orElseThrow(() -> new RuntimeException("Parallelism property required"));
      if (inEdges.size() == 0) {
        // This vertex is root vertex.
        // Fall back to setting even distribution
        irVertex.getExecutionProperties().put(NodeNamesProperty.of(EMPTY_MAP));
      } else if (isOneToOneEdge(inEdges)) {
        final Optional<HashMap<String, Integer>> property = inEdges.iterator().next().getSrc()
            .getExecutionProperties().get(NodeNamesProperty.class);
        irVertex.getExecutionProperties().put(NodeNamesProperty.of(property.get()));
      } else {
        // This IRVertex has shuffle inEdge(s), or has multiple inEdges.
        final Map<String, Integer> parentLocationShares = new HashMap<>();
        for (final IREdge edgeToIRVertex : dag.getIncomingEdgesOf(irVertex)) {
          final IRVertex parentVertex = edgeToIRVertex.getSrc();
          final Map<String, Integer> parentShares = parentVertex.getPropertyValue(NodeNamesProperty.class).get();
          final int parentParallelism = parentVertex.getPropertyValue(ParallelismProperty.class)
              .orElseThrow(() -> new RuntimeException("Parallelism property required"));
          final Map<String, Integer> shares = parentShares.isEmpty() ? getEvenShares(bandwidthSpecification.getNodes(),
              parentParallelism) : parentShares;
          for (final Map.Entry<String, Integer> element : shares.entrySet()) {
            parentLocationShares.putIfAbsent(element.getKey(), 0);
            parentLocationShares.put(element.getKey(),
                element.getValue() + parentLocationShares.get(element.getKey()));
          }
        }
        final double[] ratios = optimize(bandwidthSpecification, parentLocationShares);
        final HashMap<String, Integer> shares = new HashMap<>();
        for (int i = 0; i < bandwidthSpecification.getNodes().size(); i++) {
          shares.put(bandwidthSpecification.getNodes().get(i), (int) (ratios[i] * parallelism));
        }
        int remainder = parallelism - shares.values().stream().mapToInt(i -> i).sum();
        for (final String nodeName : shares.keySet()) {
          if (remainder == 0) {
            break;
          }
          shares.put(nodeName, shares.get(nodeName) + 1);
          remainder--;
        }
        irVertex.getExecutionProperties().put(NodeNamesProperty.of(shares));
      }
    });
  }

  /**
   * @param inEdges list of inEdges to the specific irVertex
   * @return true if and only if the irVertex has one OneToOne edge
   */
  private static boolean isOneToOneEdge(final Collection<IREdge> inEdges) {
    return inEdges.size() == 1 && inEdges.iterator().next()
          .getPropertyValue(DataCommunicationPatternProperty.class).get()
          .equals(DataCommunicationPatternProperty.Value.OneToOne);
  }

  /**
   * Computes share of parallelism that each node is responsible for.
   * @param bandwidthSpecification provides bandwidth information between nodes
   * @param parentNodeShares shares of parallelism for the parent vertex
   * @return array of fractions of parallelism that each node is responsible for
   */
  private static double[] optimize(final BandwidthSpecification bandwidthSpecification,
                                   final Map<String, Integer> parentNodeShares) {
    final int parentParallelism = parentNodeShares.values().stream().mapToInt(i -> i).sum();
    final List<String> nodeNames = bandwidthSpecification.getNodes();
    final List<LinearConstraint> constraints = new ArrayList<>();
    final int coefficientVectorSize = nodeNames.size() + 1;

    for (int i = 0; i < nodeNames.size(); i++) {
      final String nodeName = nodeNames.get(i);
      final int nodeCoefficientIndex = i + 1;
      final int parentParallelismOnThisLocation = parentNodeShares.get(nodeName);

      // Upload bandwidth
      final double[] uploadCoefficientVector = new double[coefficientVectorSize];
      uploadCoefficientVector[OBJECTIVE_COEFFICIENT_INDEX] = bandwidthSpecification.up(nodeName);
      uploadCoefficientVector[nodeCoefficientIndex] = parentParallelismOnThisLocation;
      constraints.add(new LinearConstraint(uploadCoefficientVector, Relationship.GEQ,
          parentParallelismOnThisLocation));

      // Download bandwidth
      final double[] downloadCoefficientVector = new double[coefficientVectorSize];
      downloadCoefficientVector[OBJECTIVE_COEFFICIENT_INDEX] = bandwidthSpecification.down(nodeName);
      downloadCoefficientVector[nodeCoefficientIndex] = parentParallelismOnThisLocation - parentParallelism;
      constraints.add(new LinearConstraint(downloadCoefficientVector, Relationship.GEQ, 0));

      // The coefficient is non-negative
      final double[] nonNegativeCoefficientVector = new double[coefficientVectorSize];
      nonNegativeCoefficientVector[nodeCoefficientIndex] = 1;
      constraints.add(new LinearConstraint(nonNegativeCoefficientVector, Relationship.GEQ, 0));
    }

    // The sum of all coefficient is 1
    final double[] sumCoefficientVector = new double[coefficientVectorSize];
    for (int i = 0; i < nodeNames.size(); i++) {
      sumCoefficientVector[OBJECTIVE_COEFFICIENT_INDEX + 1 + i] = 1;
    }
    constraints.add(new LinearConstraint(sumCoefficientVector, Relationship.EQ, 1));

    // Objective
    final double[] objectiveCoefficientVector = new double[coefficientVectorSize];
    objectiveCoefficientVector[OBJECTIVE_COEFFICIENT_INDEX] = 1;
    final LinearObjectiveFunction objectiveFunction = new LinearObjectiveFunction(objectiveCoefficientVector, 0);

    // Solve
    try {
      final SimplexSolver solver = new SimplexSolver();
      final Field iterations = BaseOptimizer.class.getDeclaredField("iterations");
      iterations.setAccessible(true);
      final Incrementor incrementor = (Incrementor) iterations.get(solver);
      incrementor.setMaximalCount(2147483647);
      LOG.info(String.format("Max iterations: %d", solver.getMaxIterations()));
      final PointValuePair solved = solver.optimize(
          new LinearConstraintSet(constraints), objectiveFunction, GoalType.MINIMIZE);

      return Arrays.copyOfRange(solved.getPoint(), OBJECTIVE_COEFFICIENT_INDEX + 1, coefficientVectorSize);
    } catch (final NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Bandwidth specification.
   */
  private static final class BandwidthSpecification {
    private final List<String> nodeNames = new ArrayList<>();
    private final Map<String, Integer> uplinkBandwidth = new HashMap<>();
    private final Map<String, Integer> downlinkBandwidth = new HashMap<>();

    private BandwidthSpecification() {
    }

    static BandwidthSpecification fromJsonString(final String jsonString) {
      final BandwidthSpecification specification = new BandwidthSpecification();
      try {
        final ObjectMapper objectMapper = new ObjectMapper();
        final TreeNode jsonRootNode = objectMapper.readTree(jsonString);
        for (int i = 0; i < jsonRootNode.size(); i++) {
          final TreeNode locationNode = jsonRootNode.get(i);
          final String name = locationNode.get("name").traverse().nextTextValue();
          final int up = locationNode.get("up").traverse().getIntValue();
          final int down = locationNode.get("down").traverse().getIntValue();
          specification.nodeNames.add(name);
          specification.uplinkBandwidth.put(name, up);
          specification.downlinkBandwidth.put(name, down);
        }
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
      return specification;
    }

    int up(final String nodeName) {
      return uplinkBandwidth.get(nodeName);
    }

    int down(final String nodeName) {
      return downlinkBandwidth.get(nodeName);
    }

    List<String> getNodes() {
      return nodeNames;
    }
  }
}
