/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.nemo.runtime.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Broadcast variables saved in the master.
 */
public final class InMasterBroadcastVariables {
  private static final Logger LOG = LoggerFactory.getLogger(InMasterBroadcastVariables.class.getName());
  private static final Map<Serializable, Object> TAG_TO_VARIABLE = new HashMap<>();

  private InMasterBroadcastVariables() {
  }

  public static Object getBroadcastVariable(final Serializable tag) {
    return TAG_TO_VARIABLE.get(tag);
  }

  public static void registerBroadcastVariable(final Serializable tag, final Object variable) {
    // integrity check
    if (null != TAG_TO_VARIABLE.put(tag, variable)) {
      throw new IllegalArgumentException("Cannot register duplicate tags: " + tag.toString());
    }
  }
}
