package org.apache.nemo.compiler.optimizer.policy;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.reef.tang.Injector;

import java.io.Serializable;

/**
 * An interface for policies, each of which is composed of a list of static optimization passes.
 * The list of static optimization passes are run in the order provided by the implementation.
 * Most policies follow the implementation in {@link PolicyImpl}.
 */
public interface Policy extends Serializable {
  /**
   * Optimize the DAG with the compile time optimizations.
   *
   * @param dag          input DAG.
   * @param dagDirectory directory to save the DAG information.
   * @return optimized DAG, reshaped or tagged with execution properties.
   */
  DAG<IRVertex, IREdge> runCompileTimeOptimization(DAG<IRVertex, IREdge> dag, String dagDirectory);

  /**
   * Register runtime optimizations to the event handler.
   *
   * @param injector      Tang Injector which contains the implementations of run-time event handlers.
   * @param pubSubWrapper pub-sub event handler which managing run-time and compile-time event handling.
   */
  void registerRunTimeOptimizations(Injector injector, PubSubEventHandlerWrapper pubSubWrapper);
}
