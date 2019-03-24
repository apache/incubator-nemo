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
import org.apache.nemo.common.exception.CompileTimeOptimizationException;
import org.apache.nemo.common.exception.IllegalEdgeOperationException;
import org.apache.nemo.common.exception.IllegalVertexOperationException;
import org.apache.nemo.common.exception.InvalidParameterException;
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

/**
 * Pass for applying XGBoost optimizations.
 */
@Annotates()
public final class XGBoostPass extends AnnotatingPass {
  private static final Logger LOG = LoggerFactory.getLogger(XGBoostPass.class.getName());

  /**
   * Default constructor.
   */
  public XGBoostPass() {
    super(XGBoostPass.class);
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    try {
      final String message = OptimizerUtils.takeFromMessageBuffer();
      LOG.info("Received message from the client: {}", message);

      if (message.isEmpty()) {
        return dag;
      } else {
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, String>> listOfMap =
          mapper.readValue(message, new TypeReference<List<Map<String, String>>>() {
          });
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
}
