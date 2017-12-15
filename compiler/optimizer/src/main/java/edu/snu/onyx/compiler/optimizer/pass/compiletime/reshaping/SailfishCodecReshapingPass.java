/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.onyx.compiler.optimizer.pass.compiletime.reshaping;

import edu.snu.onyx.common.coder.BytesCoder;
import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.common.dag.DAGBuilder;
import edu.snu.onyx.common.ir.edge.IREdge;
import edu.snu.onyx.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.common.ir.vertex.OperatorVertex;
import edu.snu.onyx.compiler.frontend.onyx.transform.transform.SailfishDecodingTransform;
import edu.snu.onyx.compiler.frontend.onyx.transform.transform.SailfishEncodingTransform;

import java.util.Collections;

/**
 * Pass to modify the DAG for a job to batch the disk seek.
 * It adds two {@link OperatorVertex}s with {@link SailfishEncodingTransform}
 * and {@link SailfishDecodingTransform} before and after the shuffle edges,
 * to enable the relaying vertex to receive and send data in arrays of bytes.
 *
 * If the DAG before this pass like below:
 * Map -(SG)- Reduce
 * The DAG will be processed like:
 * Map -(O2O)- SailfishEncoding -(Shuffle)- SailfishDecoding -(O2O)- Reduce
 */
public final class SailfishCodecReshapingPass extends ReshapingPass {

  public SailfishCodecReshapingPass() {
    super(Collections.singleton(ExecutionProperty.Key.DataCommunicationPattern));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    dag.topologicalDo(v -> {
      builder.addVertex(v);
      // We care about OperatorVertices that have any incoming edge that
      // has Shuffle as data communication pattern.
      if (v instanceof OperatorVertex && dag.getIncomingEdgesOf(v).stream().anyMatch(irEdge ->
              DataCommunicationPatternProperty.Value.Shuffle
          .equals(irEdge.getProperty(ExecutionProperty.Key.DataCommunicationPattern)))) {
        dag.getIncomingEdgesOf(v).forEach(edge -> {
          if (DataCommunicationPatternProperty.Value.Shuffle
                .equals(edge.getProperty(ExecutionProperty.Key.DataCommunicationPattern))
              && !edge.isSideInput()) {
            final Coder valueCoder = edge.getCoder();
            final Coder bytesCoder = new BytesCoder();
            final IRVertex shuffleSrc = edge.getSrc();
            final IRVertex shuffleDst = v;

            // Insert a encoding vertex.
            final OperatorVertex encodingVertex = new OperatorVertex(new SailfishEncodingTransform<>(valueCoder));
            builder.addVertex(encodingVertex);
            final IREdge shuffleSrcToEncoder = new IREdge(DataCommunicationPatternProperty.Value.OneToOne,
                shuffleSrc, encodingVertex, valueCoder);

            // Insert a decoding vertex.
            final OperatorVertex decodingVertex = new OperatorVertex(new SailfishDecodingTransform<>(valueCoder));
            builder.addVertex(decodingVertex);
            final IREdge encoderToDecoder = new IREdge(DataCommunicationPatternProperty.Value.Shuffle,
                encodingVertex, decodingVertex, bytesCoder);
            final IREdge decoderToShuffleDst = new IREdge(DataCommunicationPatternProperty.Value.OneToOne,
                decodingVertex, shuffleDst, valueCoder);
            edge.copyExecutionPropertiesTo(encoderToDecoder);
            builder.connectVertices(shuffleSrcToEncoder);
            builder.connectVertices(encoderToDecoder);
            builder.connectVertices(decoderToShuffleDst);
          } else {
            builder.connectVertices(edge);
          }
        });
      } else { // Others are simply added to the builder.
        dag.getIncomingEdgesOf(v).forEach(builder::connectVertices);
      }
    });
    return builder.build();
  }
}
