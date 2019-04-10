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
 * <p>
 * 1. The pass first triggers the client to run the XGBoost script, located under the `ml` python package.
 * 2. The client runs the script, which trains the tree model using the metrics collected before, and constructs
 *    a tree model, which then predicts the 'knobs' that minimizes the JCT based on the weights of the leaves (JCT).
 * 3. It receives the results, and in which direction each of the knobs should be optimized, and reconstructs the
 *    execution properties in the form that they are tuned.
 * 4. The newly reconstructed execution properties are injected and the workload runs after the optimization.
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
        listOfMap.forEach(m -> {
          final Pair<List<Object>, Integer> objAndEPKey;
          if (m.get("feature").length() == 10) {  // get by pattern.
            // Formatted into 10 digits: 0:pattern(1) 1-3:pattern ID 4-5:vtx/edge index 6-9:EP Index.
            objAndEPKey = OptimizerUtils.patternStringToObjsAndEPKeyIndex(m.get("feature"), dag);
          } else if (m.get("feature").length() == 9) {  // get by id.
            // Formatted into 9 digits: 0:vertex(2)/edge(3) 1-4:vtx/edge ID 5-8:EP Index.
            objAndEPKey = OptimizerUtils.stringToObjsAndEPKeyIndex(m.get("feature"), dag);
          } else {  // doesn't support yet.
            return;
          }
          LOG.info("Tuning: {} of {} should be {} than {}",
            objAndEPKey.right(), objAndEPKey.left(), m.get("val"), m.get("split"));
          final ExecutionProperty<? extends Serializable> newEP = MetricUtils.keyAndValueToEP(objAndEPKey.right(),
            Double.valueOf(m.get("split")), Double.valueOf(m.get("val")));
          try {
            for (final Object obj: objAndEPKey.left()) {
              if (obj instanceof IRVertex) {
                final IRVertex v = (IRVertex) obj;
                final VertexExecutionProperty<?> originalEP = v.getExecutionProperties().stream()
                  .filter(ep -> ep.getClass().isAssignableFrom(newEP.getClass())).findFirst().orElse(null);
                v.setProperty((VertexExecutionProperty) newEP);
                if (!dag.checkIntegrity().isPassed()) {
                  v.setProperty(originalEP);
                }
              } else if (obj instanceof IREdge) {
                final IREdge e = (IREdge) obj;
                final EdgeExecutionProperty<?> originalEP = e.getExecutionProperties().stream()
                  .filter(ep -> ep.getClass().isAssignableFrom(newEP.getClass())).findFirst().orElse(null);
                e.setProperty((EdgeExecutionProperty) newEP);
                if (!dag.checkIntegrity().isPassed()) {
                  e.setProperty(originalEP);
                }
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
