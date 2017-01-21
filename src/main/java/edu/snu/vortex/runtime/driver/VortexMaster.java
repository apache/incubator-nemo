package edu.snu.vortex.runtime.driver;


import edu.snu.vortex.compiler.backend.Backend;
import edu.snu.vortex.compiler.backend.vortex.VortexBackend;
import edu.snu.vortex.compiler.frontend.Frontend;
import edu.snu.vortex.compiler.frontend.beam.BeamFrontend;
import edu.snu.vortex.compiler.ir.DAG;
import edu.snu.vortex.compiler.optimizer.Optimizer;
import edu.snu.vortex.runtime.Master;
import edu.snu.vortex.runtime.TaskDAG;
import edu.snu.vortex.runtime.VortexMessage;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

public final class VortexMaster {

  private final String[] userArguments;

  @Inject
  private VortexMaster(@Parameter(Parameters.UserArguments.class) final String args) {
    userArguments = args.split(",");
  }

  public void launchJob() {
    try {
      /**
       * Step 1: Compile
       */
      System.out.println("##### VORTEX COMPILER (Frontend) #####");
      final Frontend frontend = new BeamFrontend();
      final DAG dag = frontend.compile(userArguments); // TODO #30: Use Tang to Parse User Arguments
      System.out.println(dag);

      System.out.println("##### VORTEX COMPILER (Optimizer) #####");
      final Optimizer optimizer = new Optimizer();
      final DAG optimizedDAG = optimizer.optimize(dag); // TODO #31: Interfaces for Runtime Optimization
      System.out.println(optimizedDAG);

      // TODO #28: Implement VortexBackend
      System.out.println("##### VORTEX COMPILER (Backend) #####");
      final Backend backend = new VortexBackend();
      final TaskDAG taskDAG = (TaskDAG) backend.compile(optimizedDAG);
      System.out.println(taskDAG);
      System.out.println();

      /**
       * Step 2: Execute
       */
      System.out.println();
      System.out.println("##### VORTEX Runtime #####");
      new Master(taskDAG).executeJob();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void onExecutorMessage(final VortexMessage vortexMessage) {

  }
}
