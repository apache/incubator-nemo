/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.vortex.runtime.executor.channel;

import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import org.apache.beam.sdk.transforms.DoFn;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

/**
 * Tests {@link LocalChannel}.
 */
public final class LocalChannelTest {
  private static final String CHANNEL_ID = "channel";
  private static final String DATA_TYPE = "integer";
  private ChannelConfig config;
  private final class Record implements Element<String, Integer, Integer> {
    private String type;
    private Integer key;
    private Integer value;

    public Record(final String type, final Integer key, final Integer value) {
      this.type = type;
      this.key = key;
      this.value = value;
    }

    @Override
    public String getData() {
      return type;
    }

    @Override
    public Integer getKey() {
      return key;
    }

    @Override
    public Integer getValue() {
      return value;
    }
  }

  @Before
  public void setup() {
    config = new ChannelConfig(RuntimeAttribute.Pull, 0);
  }

  @Test
  public void testReadWriteRecords() {

    final LocalChannel channel = new LocalChannel(CHANNEL_ID);
    final InputChannel inputChannel = channel;
    final OutputChannel outputChannel = channel;

    channel.initialize(config);
    List<Element> outputs = new ArrayList<>();
    int expectedValueSum = 0;
    for (int i = 0; i < 10; i++) {
      outputs.add(new Record(DATA_TYPE, i, (i * 10)));
      expectedValueSum += (i * 10);
    }
    outputChannel.write(outputs);

    Iterable<Element> inputs = inputChannel.read();
    int valueSum = 0;
    for (Element<String, Integer, Integer> record : inputs) {
      final int key = record.getKey();
      final int value = record.getValue();

      assertTrue(record.getData().compareTo(DATA_TYPE) == 0);
      assertEquals(key * 10, value);
      valueSum += record.getValue();
    }

    assertEquals(expectedValueSum, valueSum);
  }
}