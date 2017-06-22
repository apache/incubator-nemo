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
package edu.snu.vortex.compiler.frontend.beam;

import edu.snu.vortex.common.proxy.ClientEndpoint;
import edu.snu.vortex.compiler.frontend.Frontend;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.common.dag.DAG;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Frontend component for BEAM programs.
 */
public final class BeamFrontend implements Frontend {
  private static DAG dag;
  private static BeamResult beamResult;

  @Override
  public DAG<IRVertex, IREdge> compile(final String className, final String[] args) throws Exception {
    final Class userCode = Class.forName(className);
    final Method method = userCode.getMethod("main", String[].class);
    if (!Modifier.isStatic(method.getModifiers())) {
      throw new RuntimeException("User Main Method not static");
    }
    if (!Modifier.isPublic(userCode.getModifiers())) {
      throw new RuntimeException("User Main Class not public");
    }

    method.invoke(null, (Object) args);

    if (dag == null) {
      throw new IllegalStateException("DAG not supplied");
    }
    return dag;
  }

  @Override
  public ClientEndpoint getClientEndpoint() {
    if (beamResult == null) {
      throw new IllegalStateException("The Beam result not supplied.");
    }
    return beamResult;
  }

  /**
   * Supply the DAG here from the BEAM Runner.
   * @param suppliedDag the supplied DAG.
   * @param suppliedBeamResult the supplied Beam result.
   */
  static void supplyDAGFromRunner(final DAG suppliedDag,
                                  final BeamResult suppliedBeamResult) {
    if (dag != null) {
      throw new IllegalArgumentException("Cannot supply DAG twice");
    }
    dag = suppliedDag;
    beamResult = suppliedBeamResult;
  }
}
