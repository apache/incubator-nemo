package org.apache.nemo.compiler.optimizer.policy;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.CompileTimePass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.*;
import org.apache.reef.tang.Injector;

import java.util.*;

/**
 * A policy for tests.
 */
public final class TestPolicy implements Policy {
  private final Policy policy;

  public TestPolicy() {
    this(false);
  }

  public TestPolicy(final boolean testPushPolicy) {
    List<CompileTimePass> compileTimePasses = new ArrayList<>();

    if (testPushPolicy) {
      compileTimePasses.add(new ShuffleEdgePushPass());
    }

    compileTimePasses.add(new DefaultScheduleGroupPass());

    this.policy = new PolicyImpl(compileTimePasses, new ArrayList<>());
  }

  @Override
  public DAG<IRVertex, IREdge> runCompileTimeOptimization(final DAG<IRVertex, IREdge> dag, final String dagDirectory) {
    return this.policy.runCompileTimeOptimization(dag, dagDirectory);
  }

  @Override
  public void registerRunTimeOptimizations(final Injector injector, final PubSubEventHandlerWrapper pubSubWrapper) {
    this.policy.registerRunTimeOptimizations(injector, pubSubWrapper);
  }
}
