/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.compiler.backend;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.compiler.backend.nemo.NemoBackend;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.Optional;
import java.util.function.Function;

/**
 * Interface for backend components.
 * @param <Plan> the physical execution plan to compile the DAG into.
 */
@DefaultImplementation(NemoBackend.class)
public interface Backend<Plan> {
  /**
   * Compiles a DAG to a physical execution plan.
   * The method should not modify the IRDAG in any way (i.e., should be idempotent).
   *
   * @param dag the DAG to compile.
   * @param existingIdFetcher to use when assigning ids.
   * @return the execution plan generated.
   * @throws Exception Exception on the way.
   */
  Plan compile(final IRDAG dag, final Function<Object, Optional<String>> existingIdFetcher ) throws Exception;
}
