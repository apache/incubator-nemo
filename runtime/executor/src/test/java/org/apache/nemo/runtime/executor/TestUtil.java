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
package org.apache.nemo.runtime.executor;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.ir.edge.Stage;
import org.apache.nemo.runtime.common.HDFSUtils;
import org.apache.nemo.offloading.common.StateStore;
import org.apache.nemo.runtime.common.HDFStateStore;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class TestUtil {
  public static List<String> generateTaskIds(final Stage stage) {
    final List<String> result = new ArrayList<>();
    final int first_attempt = 0;
    for (final int taskIndex : stage.getTaskIndices()) {
      result.add(RuntimeIdManager.generateTaskId(stage.getId(), taskIndex, first_attempt));
    }
    return result;
  }

  @Test
  public void testHDFStore() throws IOException {
    HDFSUtils.createStateDirIfNotExistsAndDelete();

    final StateStore stateStore = new HDFStateStore();

    final ByteArrayOutputStream bos = new ByteArrayOutputStream(100);
    final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
    SerializationUtils.serialize((Serializable) list, bos);

    bos.close();

    stateStore.put("T1", bos.toByteArray());

    assertTrue(stateStore.containsState("T1"));

    final InputStream is = stateStore.getStateStream("T1");
    final List<Integer> result = SerializationUtils.deserialize(is);

    assertEquals(result, list);
  }
}
