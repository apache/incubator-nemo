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
package org.apache.nemo.compiler.frontend.spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Broadcast variables of Spark.
 */
public final class SparkBroadcastVariables {
  private static final Logger LOG = LoggerFactory.getLogger(SparkBroadcastVariables.class.getName());
  private static final AtomicLong ID_GENERATOR = new AtomicLong(0);
  private static final Map<Serializable, Object> ID_TO_VARIABLE = new HashMap<>();

  /**
   * Private constructor.
   */
  private SparkBroadcastVariables() {
  }

  /**
   * @param variable data.
   * @return the id of the variable.
   */
  public static long register(final Object variable) {
    final long id = ID_GENERATOR.getAndIncrement();
    ID_TO_VARIABLE.put(id, variable);
    LOG.info("Registered Spark broadcast variable with id {}", id);
    return id;
  }

  /**
   * @return all the map from ids to variables.
   */
  public static Map<Serializable, Object> getAll() {
    return ID_TO_VARIABLE;
  }
}
