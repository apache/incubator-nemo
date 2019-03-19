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

import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.ir.edge.executionproperty.DataFlowProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DecoderProperty;
import org.apache.nemo.common.ir.edge.executionproperty.EncoderProperty;
import org.apache.nemo.common.ir.executionproperty.ExecutionProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceSlotProperty;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;

public class MetricUtilsTest {
  private static final Logger LOG = LoggerFactory.getLogger(MetricUtilsTest.class.getName());

  @Test
  public void testEnumIndexAndValue() {
    final DataFlowProperty.Value pull = DataFlowProperty.Value.Pull;
    final DataFlowProperty.Value push = DataFlowProperty.Value.Push;

    final DataFlowProperty ep = DataFlowProperty.of(pull);
    final Integer epKeyIndex = MetricUtils.getEpKeyIndex(ep);
    final Integer idx = MetricUtils.valueToIndex(epKeyIndex, ep);
    // Pull is of ordinal index 0
    Assert.assertEquals(Integer.valueOf(0), idx);

    final Object pull1 = MetricUtils.indexToValue(0.5, -0.1, epKeyIndex);
    Assert.assertEquals(pull, pull1);
    final Object push1 = MetricUtils.indexToValue(0.5, 0.1, epKeyIndex);
    Assert.assertEquals(push, push1);
    final Object pull2 = MetricUtils.indexToValue(-0.5, -0.1, epKeyIndex);
    Assert.assertEquals(pull, pull2);
    final Object pull3 = MetricUtils.indexToValue(-0.5, 0.1, epKeyIndex);
    Assert.assertEquals(pull, pull3);
    final Object push2 = MetricUtils.indexToValue(2.0, 1.0, epKeyIndex);
    Assert.assertEquals(push, push2);
    final Object push3 = MetricUtils.indexToValue(1.1, -0.1, epKeyIndex);
    Assert.assertEquals(push, push3);
    final Object push4 = MetricUtils.indexToValue(1.1, 0.1, epKeyIndex);
    Assert.assertEquals(push, push4);
  }

  @Test
  public void testIntegerBooleanIndexAndValue() {
    final Integer one = 1;
    final Integer hundred = 100;
    final Boolean t = true;
    final Boolean f = false;

    final ParallelismProperty pEp1 = ParallelismProperty.of(one);
    final ParallelismProperty pEp100 = ParallelismProperty.of(hundred);
    final Integer pEp1KeyIndex = MetricUtils.getEpKeyIndex(pEp1);
    final Integer pEp100KeyIndex = MetricUtils.getEpKeyIndex(pEp100);
    Assert.assertEquals(Integer.valueOf(1), MetricUtils.valueToIndex(pEp1KeyIndex, pEp1));
    Assert.assertEquals(Integer.valueOf(100), MetricUtils.valueToIndex(pEp100KeyIndex, pEp100));


    final ResourceSlotProperty rsEpT = ResourceSlotProperty.of(t);
    final ResourceSlotProperty rsEpF = ResourceSlotProperty.of(f);
    final Integer rsEpTKeyIndex = MetricUtils.getEpKeyIndex(rsEpT);
    final Integer rsEpFKeyIndex = MetricUtils.getEpKeyIndex(rsEpF);
    Assert.assertEquals(Integer.valueOf(1), MetricUtils.valueToIndex(rsEpTKeyIndex, rsEpT));
    Assert.assertEquals(Integer.valueOf(0), MetricUtils.valueToIndex(rsEpFKeyIndex, rsEpF));

    final Object one1 = MetricUtils.indexToValue(1.5, -0.1, pEp1KeyIndex);
    final Object one2 = MetricUtils.indexToValue(0.5, 0.1, pEp1KeyIndex);
    final Object one3 = MetricUtils.indexToValue(2.0, -0.6, pEp1KeyIndex);
    final Object one4 = MetricUtils.indexToValue(0.0, 0.5, pEp1KeyIndex);
    Assert.assertEquals(one, one1);
    Assert.assertEquals(one, one2);
    Assert.assertEquals(one, one3);
    Assert.assertEquals(one, one4);

    final Object hundred1 = MetricUtils.indexToValue(100.5, -0.1, pEp100KeyIndex);
    final Object hundred2 = MetricUtils.indexToValue(99.5, 0.1, pEp100KeyIndex);
    Assert.assertEquals(hundred, hundred1);
    Assert.assertEquals(hundred, hundred2);

    final Object t1 = MetricUtils.indexToValue(1.5, -0.1, rsEpTKeyIndex);
    final Object t2 = MetricUtils.indexToValue(0.1, 0.5, rsEpTKeyIndex);
    final Object t3 = MetricUtils.indexToValue(1.5, 0.1, rsEpTKeyIndex);
    Assert.assertEquals(true, t1);
    Assert.assertEquals(true, t2);
    Assert.assertEquals(true, t3);

    final Object f1 = MetricUtils.indexToValue(0.5, -0.1, rsEpFKeyIndex);
    final Object f2 = MetricUtils.indexToValue(-0.5, 0.1, rsEpFKeyIndex);
    final Object f3 = MetricUtils.indexToValue(-0.5, -0.1, rsEpFKeyIndex);
    Assert.assertEquals(false, f1);
    Assert.assertEquals(false, f2);
    Assert.assertEquals(false, f3);
  }

  @Test
  public void testOtherIndexAndValue() {
    final EncoderFactory ef = new EncoderFactory.DummyEncoderFactory();
    final DecoderFactory df = new DecoderFactory.DummyDecoderFactory();

    final EncoderProperty eEp = EncoderProperty.of(ef);
    final DecoderProperty dEp = DecoderProperty.of(df);
    final Integer eEpKeyIndex = MetricUtils.getEpKeyIndex(eEp);
    final Integer dEpKeyIndex = MetricUtils.getEpKeyIndex(dEp);
    final Integer efidx = MetricUtils.valueToIndex(eEpKeyIndex, eEp);
    final Integer dfidx = MetricUtils.valueToIndex(dEpKeyIndex, dEp);

    final Object ef1 = MetricUtils.indexToValue(0.1 + efidx, -0.1, eEpKeyIndex);
    final Object ef2 = MetricUtils.indexToValue(-0.1 + efidx, 0.1, eEpKeyIndex);
    Assert.assertEquals(ef, ef1);
    Assert.assertEquals(ef, ef2);

    final Object df1 = MetricUtils.indexToValue(0.1 + dfidx, -0.1, dEpKeyIndex);
    final Object df2 = MetricUtils.indexToValue(-0.1 + dfidx, 0.1, dEpKeyIndex);
    Assert.assertEquals(df, df1);
    Assert.assertEquals(df, df2);
  }

  @Test
  public void testPairAndValueToEP() {
    final DataFlowProperty.Value pull = DataFlowProperty.Value.Pull;
    final DataFlowProperty ep = DataFlowProperty.of(pull);
    final Integer epKeyIndex = MetricUtils.getEpKeyIndex(ep);
    final Integer idx = MetricUtils.valueToIndex(epKeyIndex, ep);
    Assert.assertEquals(Integer.valueOf(0), idx);

    final ExecutionProperty<? extends Serializable> ep2 =
      MetricUtils.pairAndValueToEP(epKeyIndex, 0.5, -0.1);
    Assert.assertEquals(ep, ep2);
  }
}
