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
package edu.snu.onyx.compiler.frontend;

import edu.snu.onyx.common.proxy.ClientEndpoint;
import edu.snu.onyx.compiler.ir.IREdge;
import edu.snu.onyx.compiler.ir.IRVertex;
import edu.snu.onyx.common.dag.DAG;

/**
 * Interface for the frontend class.
 */
public interface Frontend {
  /**
   * Compile the given program to an IR DAG.
   * @param className user main class.
   * @param args arguments.
   * @return the compiled IR DAG.
   * @throws Exception Exception on the way.
   */
  DAG<IRVertex, IREdge> compile(String className, String[] args) throws Exception;

  /**
   * Get the {@link ClientEndpoint} of the given program.
   * @return the client endpoint.
   */
  ClientEndpoint getClientEndpoint();
}
