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
import edu.snu.nemo.common.ir.vertex.executionproperty.LocationSharesProperty;
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
 * Computes and assigns appropriate share of locations to each stage,
 * with respect to bandwidth restrictions of locations. If bandwidth information is not given, this pass does nothing.
 *
 * <h3>Assumptions</h3>
 * This pass assumes no skew in input or intermediate data, so that the number of TaskGroups assigned to a location
 * is proportional to the data size handled by the location.
 * Also, this pass assumes stages with empty map as {@link LocationSharesProperty} are assigned to locations evenly.
 * For example, if source splits are not distributed evenly, any source location-aware scheduling policy will
 * assign TaskGroups unevenly.
 * Also, this pass assumes network bandwidth to be the bottleneck. Each location should have enough capacity to run
 * TaskGroups immediately as scheduler attempts to schedule a TaskGroup.
 */
public final class LocationShareAssignmentPass extends AnnotatingPass {

  private static final int OBJECTIVE_COEFFICIENT_INDEX = 0;
  private static final Logger LOG = LoggerFactory.getLogger(LocationShareAssignmentPass.class);
  private static final HashMap<String, Integer> EMPTY_MAP = new HashMap<>();

  private static String bandwidthSpecificationString = "";


  /**
   * Default constructor.
   */
  public LocationShareAssignmentPass() {
    super(LocationSharesProperty.class, Collections.singleton(ParallelismProperty.class));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    if (bandwidthSpecificationString.isEmpty()) {
      dag.topologicalDo(irVertex -> irVertex.setProperty(LocationSharesProperty.of(EMPTY_MAP)));
    } else {
      assignLocationShares(dag, BandwidthSpecification.fromJsonString(bandwidthSpecificationString));
    }
    return dag;
  }

  public static void setBandwidthSpecificationString(final String value) {
    bandwidthSpecificationString = value;
  }

  private static void assignLocationShares(
      final DAG<IRVertex, IREdge> dag,
      final BandwidthSpecification bandwidthSpecification) {
    dag.topologicalDo(irVertex -> {
      final Collection<IREdge> inEdges = dag.getIncomingEdgesOf(irVertex);
      final int parallelism = irVertex.getPropertyValue(ParallelismProperty.class)
          .orElseThrow(() -> new RuntimeException("Parallelism property required"));
      if (inEdges.size() == 0) {
        // The stage is root stage.
        // Fall back to setting even distribution
        final HashMap<String, Integer> shares = new HashMap<>();
        final List<String> locations = bandwidthSpecification.getLocations();
        final int defaultShare = parallelism / locations.size();
        final int remainder = parallelism % locations.size();
        for (int i = 0; i < locations.size(); i++) {
          shares.put(locations.get(i), defaultShare + (i < remainder ? 1 : 0));
        }
        irVertex.getExecutionProperties().put(LocationSharesProperty.of(shares));
      } else if (inEdges.size() == 1 && inEdges.iterator().next()
          .getPropertyValue(DataCommunicationPatternProperty.class).get()
          .equals(DataCommunicationPatternProperty.Value.OneToOne)) {
        final Optional<LocationSharesProperty> property = dag.getIncomingEdgesOf(irVertex).iterator().next()
            .getExecutionProperties().get(LocationSharesProperty.class);
        irVertex.getExecutionProperties().put(property.get());
      } else {
        final Map<String, Integer> parentLocationShares = new HashMap<>();
        for (final IREdge edgeToIRVertex : dag.getIncomingEdgesOf(irVertex)) {
          final IRVertex parentVertex = edgeToIRVertex.getSrc();
          final Map<String, Integer> shares = parentVertex.getPropertyValue(LocationSharesProperty.class).get();
          for (final Map.Entry<String, Integer> element : shares.entrySet()) {
            parentLocationShares.putIfAbsent(element.getKey(), 0);
            parentLocationShares.put(element.getKey(),
                element.getValue() + parentLocationShares.get(element.getKey()));
          }
        }
        final double[] ratios = optimize(bandwidthSpecification, parentLocationShares);
        final HashMap<String, Integer> shares = new HashMap<>();
        for (int i = 0; i < bandwidthSpecification.getLocations().size(); i++) {
          shares.put(bandwidthSpecification.getLocations().get(i), (int) (ratios[i] * parallelism));
        }
        int remainder = parallelism - shares.values().stream().mapToInt(i -> i).sum();
        for (final String location : shares.keySet()) {
          if (remainder == 0) {
            break;
          }
          shares.put(location, shares.get(location) + 1);
          remainder--;
        }
        irVertex.getExecutionProperties().put(LocationSharesProperty.of(shares));
      }
    });
  }

  private static double[] optimize(final BandwidthSpecification bandwidthSpecification,
                                   final Map<String, Integer> parentLocationShares) {
    final int parentParallelism = parentLocationShares.values().stream().mapToInt(i -> i).sum();
    final List<String> locations = bandwidthSpecification.getLocations();
    final List<LinearConstraint> constraints = new ArrayList<>();
    final int coefficientVectorSize = locations.size() + 1;

    for (int i = 0; i < locations.size(); i++) {
      final String location = locations.get(i);
      final int locationCoefficientIndex = i + 1;
      final int parentParallelismOnThisLocation = parentLocationShares.get(location);

      // Upload bandwidth
      final double[] uploadCoefficientVector = new double[coefficientVectorSize];
      uploadCoefficientVector[OBJECTIVE_COEFFICIENT_INDEX] = bandwidthSpecification.up(location);
      uploadCoefficientVector[locationCoefficientIndex] = parentParallelismOnThisLocation;
      constraints.add(new LinearConstraint(uploadCoefficientVector, Relationship.GEQ,
          parentParallelismOnThisLocation));

      // Download bandwidth
      final double[] downloadCoefficientVector = new double[coefficientVectorSize];
      downloadCoefficientVector[OBJECTIVE_COEFFICIENT_INDEX] = bandwidthSpecification.down(location);
      downloadCoefficientVector[locationCoefficientIndex] = parentParallelismOnThisLocation - parentParallelism;
      constraints.add(new LinearConstraint(downloadCoefficientVector, Relationship.GEQ, 0));

      // The coefficient is non-negative
      final double[] nonNegativeCoefficientVector = new double[coefficientVectorSize];
      nonNegativeCoefficientVector[locationCoefficientIndex] = 1;
      constraints.add(new LinearConstraint(nonNegativeCoefficientVector, Relationship.GEQ, 0));
    }

    // The sum of all coefficient is 1
    final double[] sumCoefficientVector = new double[coefficientVectorSize];
    for (int i = 0; i < locations.size(); i++) {
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
    private final List<String> locations = new ArrayList<>();
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
          specification.locations.add(name);
          specification.uplinkBandwidth.put(name, up);
          specification.downlinkBandwidth.put(name, down);
        }
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
      return specification;
    }

    int up(final String location) {
      return uplinkBandwidth.get(location);
    }

    int down(final String location) {
      return downlinkBandwidth.get(location);
    }

    List<String> getLocations() {
      return locations;
    }
  }
}
