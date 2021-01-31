package org.apache.nemo.runtime.executor.task;

import org.apache.nemo.common.KeyExtractor;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.PairKeyExtractor;
import org.apache.nemo.common.coder.*;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.Stage;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;
import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.test.EmptyComponents;
import org.apache.nemo.compiler.frontend.beam.transform.FlattenTransform;
import org.apache.nemo.compiler.optimizer.policy.BasicPullPolicy;
import org.apache.nemo.compiler.optimizer.policy.BasicPushPolicy;
import org.apache.nemo.compiler.optimizer.policy.Policy;
import org.apache.nemo.compiler.optimizer.policy.StreamingPolicy;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.PhysicalPlanGenerator;
import org.apache.nemo.runtime.executor.task.util.*;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class TestDAGBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(TestDAGBuilder.class.getName());

  /**
   * Type of the plan to generate.
   */
  public enum PlanType {
    TwoVertices,
  }

  private static final String EMPTY_DAG_DIRECTORY = "";

  private final PhysicalPlanGenerator planGenerator;
  private final StreamingPolicy policy;

  public final int parallelism;

  public TestGBKTransform finalTransform;

  public TestDAGBuilder(final PhysicalPlanGenerator planGenerator,
                        final int parallelism) {
    this.planGenerator = planGenerator;
    this.policy = new StreamingPolicy();
    this.parallelism = parallelism;
    policy.build(parallelism);
  }

  public PhysicalPlan generatePhysicalPlan(final PlanType planType) throws Exception {

    switch (planType) {
      case TwoVertices:
        return convertIRToPhysical(policy.runCompileTimeOptimization(getTwoVerticesDAG(), EMPTY_DAG_DIRECTORY));
      default:
        throw new IllegalArgumentException(planType.toString());
    }
  }

  private PhysicalPlan convertIRToPhysical(final IRDAG irDAG) throws Exception {
    final DAG<Stage, StageEdge> physicalDAG = planGenerator.apply(irDAG);
    return new PhysicalPlan("TestPlan", physicalDAG);
  }

  /**
   * @return a dag that joins two vertices.
   */
  private IRDAG getTwoVerticesDAG() {
    final DAGBuilder<IRVertex, IREdge> dagBuilder = new DAGBuilder<>();
    final IRVertex src = createSource(parallelism);
    src.setProperty(ParallelismProperty.of(parallelism));

    final Transform t = new FlattenTransform();
    final IRVertex v1 = new OperatorVertex(t);
    v1.setProperty(ParallelismProperty.of(parallelism));

    final IRVertex v2 = new OperatorVertex(new TestGBKTransform());
    v2.isStateful = true;
    v2.setProperty(ParallelismProperty.of(parallelism));

    return new IRDAG(dagBuilder.addVertex(src)
      .addVertex(v1)
      .addVertex(v2)
      .connectVertices(createEdge(src, v1, CommunicationPatternProperty.Value.OneToOne))
      .connectVertices(createEdge(v1, v2, CommunicationPatternProperty.Value.Shuffle))
      .buildWithoutSourceSinkCheck());
  }

  public static IRVertex createSource(final int parallelism) {
    final List<Readable> readables = IntStream.range(0, parallelism)
      .boxed().map(l -> new TCPSourceReadable(l)).collect(Collectors.toList());

    final IRVertex sourceIRVertex = new TestUnboundedSourceVertex(readables);
    return sourceIRVertex;
  }

  private RuntimeEdge<IRVertex> createInnerEdge(final IRVertex src,
                                                final IRVertex dst,
                                                final String runtimeIREdgeId) {
    ExecutionPropertyMap<EdgeExecutionProperty> edgeProperties = new
      ExecutionPropertyMap<>(runtimeIREdgeId);
    edgeProperties.put(DataStoreProperty.of(DataStoreProperty.Value.Pipe));
    return new RuntimeEdge<>(runtimeIREdgeId, edgeProperties, src, dst);
  }

  public static IREdge createEdge(final IRVertex src, final IRVertex dst,
                                  final CommunicationPatternProperty.Value comm) {
    // CommunicationPatternProperty.Value.Shuffle
    final IREdge edge = new IREdge(comm, src, dst);
    edge.setProperty(DataStoreProperty.of(DataStoreProperty.Value.Pipe));
    edge.setProperty(KeyExtractorProperty.of(new PairKeyExtractor()));
    edge.setProperty(KeyEncoderProperty.of(IntEncoderFactory.of()));
    edge.setProperty(KeyDecoderProperty.of(IntDecoderFactory.of()));
    edge.setProperty(EncoderProperty.of(PairEncoderFactory.of(IntEncoderFactory.of(), IntEncoderFactory.of())));
    edge.setProperty(DecoderProperty.of(PairDecoderFactory.of(IntDecoderFactory.of(), IntDecoderFactory.of())));
    return edge;
  }
}
