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

import edu.snu.vortex.compiler.backend.Backend;
import edu.snu.vortex.compiler.backend.vortex.VortexBackend;
import edu.snu.vortex.compiler.frontend.Frontend;
import edu.snu.vortex.compiler.frontend.beam.BeamFrontend;
import edu.snu.vortex.compiler.optimizer.Optimizer;
import edu.snu.vortex.compiler.ir.DAG;
import edu.snu.vortex.engine.SimpleEngine;
import edu.snu.vortex.runtime.Master;
import edu.snu.vortex.runtime.TaskDAG;

public final class JobLauncher {
  public static void main(final String[] args) throws Exception {
    /**
     * Step 1: Compile
     */
    System.out.println("##### VORTEX COMPILER (Frontend) #####");
    final Frontend frontend = new BeamFrontend();
    final DAG dag = frontend.compile(args); // TODO #30: Use Tang to Parse User Arguments
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
    System.out.println("##### VORTEX ENGINE #####");
    new SimpleEngine().executeDAG(optimizedDAG);

    System.out.println();
    System.out.println("##### VORTEX Runtime #####");
    new Master(taskDAG).executeJob();
  }
}
