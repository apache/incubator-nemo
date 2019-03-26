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

package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.exception.*;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;
import org.apache.nemo.common.ir.executionproperty.ExecutionProperty;
import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.optimizer.OptimizerUtils;
import org.apache.nemo.runtime.common.metric.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Pass for applying XGBoost optimizations.
 */
@Annotates()
public final class XGBoostPass extends AnnotatingPass {
  private static final Logger LOG = LoggerFactory.getLogger(XGBoostPass.class.getName());

  private static final BlockingQueue<String> MESSAGE_QUEUE = new LinkedBlockingQueue<>();

  /**
   * Default constructor.
   */
  public XGBoostPass() {
    super(XGBoostPass.class);
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    try {
      final String message = XGBoostPass.takeMessage();
      LOG.info("Received message from the client: {}", message);

      if (message.isEmpty()) {
        LOG.info("No optimization included in the message. Returning the original dag.");
        return dag;
      } else {
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, String>> listOfMap =
          mapper.readValue(message, new TypeReference<List<Map<String, String>>>() {
          });
        // Formatted into 9 digits: 0:vertex/edge 1-5:ID 5-9:EP Index.
        listOfMap.stream().filter(m -> m.get("feature").length() == 9).forEach(m -> {
          final Pair<String, Integer> idAndEPKey = OptimizerUtils.stringToIdAndEPKeyIndex(m.get("feature"));
          LOG.info("Tuning: {} of {} should be {} than {}",
            idAndEPKey.right(), idAndEPKey.left(), m.get("val"), m.get("split"));
          final ExecutionProperty<? extends Serializable> newEP = MetricUtils.pairAndValueToEP(idAndEPKey.right(),
            Double.valueOf(m.get("split")), Double.valueOf(m.get("val")));
          try {
            if (idAndEPKey.left().startsWith("vertex")) {
              final IRVertex v = dag.getVertexById(idAndEPKey.left());
              final VertexExecutionProperty<?> originalEP = v.getExecutionProperties().stream()
                .filter(ep -> ep.getClass().isAssignableFrom(newEP.getClass())).findFirst().orElse(null);
              v.setProperty((VertexExecutionProperty) newEP);
              if (!dag.checkIntegrity().isPassed()) {
                v.setProperty(originalEP);
              }
            } else if (idAndEPKey.left().startsWith("edge")) {
              final IREdge e = dag.getEdgeById(idAndEPKey.left());
              final EdgeExecutionProperty<?> originalEP = e.getExecutionProperties().stream()
                .filter(ep -> ep.getClass().isAssignableFrom(newEP.getClass())).findFirst().orElse(null);
              e.setProperty((EdgeExecutionProperty) newEP);
              if (!dag.checkIntegrity().isPassed()) {
                e.setProperty(originalEP);
              }
            }
          } catch (IllegalVertexOperationException | IllegalEdgeOperationException e) {
          }
        });
      }
    } catch (final InvalidParameterException e) {
      LOG.warn(e.getMessage());
      return dag;
    } catch (final Exception e) {
      throw new CompileTimeOptimizationException(e);
    }

    return dag;
  }

  /**
   * @param message push the message to the message queue.
   */
  public static void pushMessage(final String message) {
    MESSAGE_QUEUE.add(message);
  }

  /**
   * @return the message from the blocking queue.
   */
  private static String takeMessage() {
    try {
      return MESSAGE_QUEUE.take();
    } catch (InterruptedException e) {
      throw new MetricException("Interrupted while waiting for message: " + e);
    }
  }
}
