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

package org.apache.nemo.runtime.executor;

import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.runtime.executor.data.BroadcastManagerWorker;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link TransformContextImpl}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BroadcastManagerWorker.class})
public class TransformContextImplTest {
  private Transform.Context context;
  private final Map<String, String> taggedOutputs = new HashMap<>();

  @Before
  public void setUp() {
    final BroadcastManagerWorker broadcastManagerWorker = mock(BroadcastManagerWorker.class);
    when(broadcastManagerWorker.get("a")).thenReturn("b");
    this.context = new TransformContextImpl(broadcastManagerWorker, taggedOutputs);
  }

  @Test
  public void testContextImpl() {
    assertEquals("b", this.context.getBroadcastVariable("a"));
    assertEquals(this.taggedOutputs, this.context.getTagToAdditionalChildren());

    final String sampleText = "test_text";

    assertFalse(this.context.getSerializedData().isPresent());

    this.context.setSerializedData(sampleText);
    assertTrue(this.context.getSerializedData().isPresent());
    assertEquals(sampleText, this.context.getSerializedData().get());
  }
}
