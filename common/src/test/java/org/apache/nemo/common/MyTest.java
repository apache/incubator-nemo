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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link Pair}.
 */
public class MyTest {


  @Test
  public void testResourceSpecBuilder() {

    final String spec = ResourceSpecBuilder.builder()
      .addResource(ResourceSpecBuilder.ResourceType.Reserved, 512, 5)
      .addResource(ResourceSpecBuilder.ResourceType.Transient, 512, 5)
      .build();

    final String s = "[{\"type\":\"Reserved\",\"memory_mb\":512,\"capacity\":5},"
      + "{\"type\":\"Transient\",\"memory_mb\":512,\"capacity\":5}]";

    assertEquals(s, spec);

  }
}
