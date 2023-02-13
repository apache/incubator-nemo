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
package org.apache.nemo.common;

import org.apache.hadoop.mapred.JobConf;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;

import java.util.LinkedList;
import java.util.List;

public final class ResourceSpecBuilder {

  public enum ResourceType {
    Transient,
    Reserved
  }

  private final List<String> resources = new LinkedList<>();

  public static ResourceSpecBuilder builder() {
    return new ResourceSpecBuilder();
  }

  public ResourceSpecBuilder addResource(final ResourceType resourceType,
                                         final int memory_mb,
                                         final int capacity) {
    return addResource(resourceType, memory_mb, capacity, 1, 1);
  }


  public ResourceSpecBuilder addResource(final ResourceType resourceType,
                                         final int memory_mb,
                                         final int capacity,
                                         final int taskSlot,
                                         final int numExecutor) {

    resources.add(String.format("{\"type\":\"%s\",\"memory_mb\":%d,\"capacity\":%d,\"slot\":%d,\"num\":%d}",
      resourceType.name(), memory_mb, capacity, taskSlot, numExecutor));
    return this;
  }

  public String build() {
    return "[" + String.join(",", resources) + "]";
  }
}