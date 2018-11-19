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
package org.apache.nemo.runtime.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Broadcast variables saved in the master.
 */
public final class BroadcastManagerMaster {
  private static final Logger LOG = LoggerFactory.getLogger(BroadcastManagerMaster.class.getName());
  private static final Map<Serializable, Object> ID_TO_VARIABLE = new HashMap<>();

  private BroadcastManagerMaster() {
  }

  /**
   * @param variables from the client.
   */
  public static void registerBroadcastVariablesFromClient(final Map<Serializable, Object> variables) {
    LOG.info("Registered broadcast variable ids {} sent from the client", variables.keySet());
    ID_TO_VARIABLE.putAll(variables);
  }

  /**
   * @param id of the broadcast variable.
   * @return the requested broadcast variable.
   */
  public static Object getBroadcastVariable(final Serializable id) {
    return ID_TO_VARIABLE.get(id);
  }
}
