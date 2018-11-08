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

import org.apache.nemo.common.coder.BytesEncoderFactory;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.edge.executionproperty.DecoderProperty;
import org.apache.nemo.common.ir.edge.executionproperty.EncoderProperty;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.RelayTransform;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Pass to modify the DAG for a job to batch the disk seek.
 * It adds a {@link OperatorVertex} with {@link RelayTransform} before the vertices
 * receiving shuffle edges,
 * to merge the shuffled data in memory and write to the disk at once.
 */@Requires(CommunicationPatternProperty.class)

public final class LargeShuffleRelayReshapingPass extends ReshapingPass {
  private static final Logger LOG = LoggerFactory.getLogger(LargeShuffleRelayReshapingPass.class.getName());

  /**
   * Default constructor.
   */
  public LargeShuffleRelayReshapingPass() {
    super(LargeShuffleRelayReshapingPass.class);
  }

  /**
   * Reshaping.
   * If A --> B is shuffle,
   *
   * Convert
   *
   *          shuffle
   * A --(Encoder1, Decoder1)--> B
   *
   * to
   *
   *                       shuffle                                     one-to-one
   * A --(LengthPaddingEncoder, LengthPaddingDecoder)-> Relay --(ByteEncoder, Decoder1)-> B
   *                        edge1                                       edge2
   * @param dag current dag
   * @return reshaping dag
   */
  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    dag.topologicalDo(B -> {
      builder.addVertex(B);
      // We care about OperatorVertices that have any incoming edge that
      // has Shuffle as data communication pattern.
      if (B instanceof OperatorVertex && dag.getIncomingEdgesOf(B).stream().anyMatch(irEdge ->
              CommunicationPatternProperty.Value.Shuffle
          .equals(irEdge.getPropertyValue(CommunicationPatternProperty.class).get()))) {
        dag.getIncomingEdgesOf(B).forEach(edge -> {
          if (CommunicationPatternProperty.Value.Shuffle
                .equals(edge.getPropertyValue(CommunicationPatternProperty.class).get())) {
            // Insert a merger vertex having transform that write received data immediately
            // before the vertex receiving shuffled data.
            final OperatorVertex iFileMergerVertex = new OperatorVertex(new RelayTransform());

            builder.addVertex(iFileMergerVertex);


            // edge 1 setting
            final IREdge edge1 =
              new IREdge(CommunicationPatternProperty.Value.Shuffle, edge.getSrc(), iFileMergerVertex);
            edge.copyExecutionPropertiesTo(edge1);

            final EncoderFactory encoderFactory = new LengthPaddingEncoderFactory(
              edge1.getPropertyValue(EncoderProperty.class).get());
            final DecoderFactory decoderFactory = new LengthPaddingDecoderFactory();

            edge1.setPropertyPermanently(EncoderProperty.of(encoderFactory));
            edge1.setPropertyPermanently(DecoderProperty.of(decoderFactory));

            // edge 2 setting
            final IREdge edge2 = new IREdge(CommunicationPatternProperty.Value.OneToOne,
                iFileMergerVertex, B);
            edge2.setPropertyPermanently(EncoderProperty.of(BytesEncoderFactory.of()));
            edge2.setPropertyPermanently(DecoderProperty.of(edge.getPropertyValue(DecoderProperty.class).get()));
            builder.connectVertices(edge1);
            builder.connectVertices(edge2);
          } else {
            builder.connectVertices(edge);
          }
        });
      } else { // Others are simply added to the builder.
        dag.getIncomingEdgesOf(B).forEach(builder::connectVertices);
      }
    });
    return builder.build();
  }

  final class LengthPaddingEncoderFactory implements EncoderFactory {

    final EncoderFactory valueEncoderFactory;

    LengthPaddingEncoderFactory(final EncoderFactory valueEncoderFactory) {
      this.valueEncoderFactory = valueEncoderFactory;
    }

    @Override
    public Encoder create(final OutputStream outputStream) throws IOException {
      final ByteArrayOutputStream bos = new ByteArrayOutputStream();
      return new LengthPaddingEncoder(
        valueEncoderFactory.create(bos), outputStream, bos);
    }
  }

  final class LengthPaddingDecoderFactory implements DecoderFactory {

    LengthPaddingDecoderFactory() {
    }

    @Override
    public Decoder create(InputStream inputStream) throws IOException {
      return new LengthPaddingDecoder(inputStream);
    }
  }

  final class LengthPaddingEncoder implements EncoderFactory.Encoder {

    private final EncoderFactory.Encoder valueEncoder;
    private final OutputStream outputStream;
    private final ByteArrayOutputStream bos;

    private LengthPaddingEncoder(final EncoderFactory.Encoder valueEncoder,
                                 final OutputStream outputStream,
                                 final ByteArrayOutputStream bos) {
      this.valueEncoder = valueEncoder;
      this.outputStream = outputStream;
      this.bos = bos;
    }

    @Override
    public void encode(Object element) throws IOException {
      // The value encoder will encode the value to the bos
      LOG.info("Encode");
      valueEncoder.encode(element);
      // close bos
      bos.close();
      final byte[] arr = bos.toByteArray();
      LOG.info("Encode length: {}, {}", arr.length, element);
      outputStream.write(arr.length);
      outputStream.write(arr);
    }
  }

  final class LengthPaddingDecoder implements DecoderFactory.Decoder {

    private final InputStream inputStream;

    private LengthPaddingDecoder(final InputStream inputStream) {
      this.inputStream = inputStream;
    }

    @Override
    public Object decode() throws IOException {
      // this just returns byte array
      final int len = inputStream.read();
      final byte[] arr = new byte[len];
      return inputStream.read(arr);
    }
  }
}
