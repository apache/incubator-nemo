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

import org.apache.nemo.common.eventhandler.OffloadingDataEvent;
import org.apache.nemo.common.eventhandler.OffloadingResultEvent;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.offloading.common.OffloadingOutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public final class OffloadingResultCollector{

  // vertexId, edgeId, data
  public final List<Triple<List<String>, String, Object>> result;
  public final OffloadingOutputCollector collector;

  public OffloadingResultCollector(final OffloadingOutputCollector collector) {
    this.result = new LinkedList<>();
    this.collector = collector;
  }

  public void flush() {
    collector.emit(new OffloadingResultEvent(result));
  }
}
