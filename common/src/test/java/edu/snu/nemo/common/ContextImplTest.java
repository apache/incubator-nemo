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

package edu.snu.nemo.common;

import edu.snu.nemo.common.ir.vertex.transform.Transform;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link ContextImpl}.
 */
public class ContextImplTest {
  private Transform.Context context;
  private final Map sideInputs = new HashMap();
  private final Optional<String> mainTag = Optional.empty();
  private final Map<String, String> taggedOutputs = new HashMap();

  @Before
  public void setUp() {
    sideInputs.put("a", "b");
    this.context = new ContextImpl(sideInputs, mainTag, taggedOutputs);
  }

  @Test
  public void testContextImpl() {
    assertEquals(this.sideInputs, this.context.getSideInputs());
    assertEquals(this.taggedOutputs, this.context.getTagToAdditionalChildren());

    final String sampleText = "test_text";

    assertFalse(this.context.getSerializedData().isPresent());

    this.context.setSerializedData(sampleText);
    assertTrue(this.context.getSerializedData().isPresent());
    assertEquals(sampleText, this.context.getSerializedData().get());
  }
}
