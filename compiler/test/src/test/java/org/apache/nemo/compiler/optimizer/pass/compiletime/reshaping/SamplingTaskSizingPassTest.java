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
package org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.compiler.CompilerTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test {@link SamplingTaskSizingPass}.
 */
@RunWith(PowerMockRunner.class)
public class SamplingTaskSizingPassTest {
  private IRDAG compiledALSDAG;
  private IRDAG compiledMLRDAG;
  private IRDAG compiledWordCountDAG;

  @Before
  public void setUp() throws Exception {
    compiledALSDAG = CompilerTestUtil.compileALSDAG();
    compiledMLRDAG = CompilerTestUtil.compileMLRDAG();
    compiledWordCountDAG = CompilerTestUtil.compileWordCountDAG();
  }

  @Test
  public void testALS() {
    final IRDAG processedDAG = new SamplingTaskSizingPass().apply(compiledALSDAG);
  }

  @Test
  public void testMLR() {
    final IRDAG processedDAG = new SamplingTaskSizingPass().apply(compiledMLRDAG);
  }

  @Test
  public void testWordCount() {
    final IRDAG processedDAG = new SamplingTaskSizingPass().apply(compiledWordCountDAG);
  }
}
