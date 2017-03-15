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
package edu.snu.vortex.client;

import edu.snu.vortex.compiler.frontend.Frontend;
import edu.snu.vortex.compiler.frontend.beam.BeamFrontend;
import edu.snu.vortex.compiler.ir.DAG;
import edu.snu.vortex.compiler.optimizer.Optimizer;
import edu.snu.vortex.engine.SimpleEngine;

/**
 * Job launcher.
 */
public final class JobLauncher {
  private JobLauncher() {
  }

  public static void main(final String[] args) throws Exception {
    /**
     * Step 1: Compile
     */
    final Frontend frontend = new BeamFrontend();
    final DAG dag = frontend.compile(args); // TODO #30: Use Tang to Parse User Arguments
    System.out.println("##### VORTEX COMPILER (Before Optimization) #####");
    System.out.println(dag);

    final Optimizer optimizer = new Optimizer();
    // TODO #31: Interfaces for Runtime Optimization
    final Optimizer.PolicyType optimizationPolicy = Optimizer.PolicyType.Pado;
    final DAG optimizedDAG = optimizer.optimize(dag, optimizationPolicy);
    System.out.println("##### VORTEX COMPILER (After Optimization for " + optimizationPolicy + ") #####");
    System.out.println(optimizedDAG);

    // TODO #28: Implement VortexBackend
    // final Backend backend = new VortexBackend();
    // final ??? vortexJobDAG = backend.compile(optimized);

    /**
     * Step 2: Execute
     */
    System.out.println("##### VORTEX ENGINE #####");
    new SimpleEngine().executeDAG(optimizedDAG);
  }
}
