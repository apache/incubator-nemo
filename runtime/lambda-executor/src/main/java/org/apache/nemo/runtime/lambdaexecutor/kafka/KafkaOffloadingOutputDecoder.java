package org.apache.nemo.runtime.lambdaexecutor.kafka;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.nemo.offloading.common.OffloadingDecoder;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultEvent;
import org.apache.nemo.runtime.lambdaexecutor.Triple;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class KafkaOffloadingOutputDecoder implements OffloadingDecoder<Object> {

  final Map<String, Serializer> serializerMap;
  private final Coder<UnboundedSource.CheckpointMark> coder;

  public KafkaOffloadingOutputDecoder(final Map<String, Serializer> serializerMap,
                                      final Coder<UnboundedSource.CheckpointMark> coder) {
    this.serializerMap = serializerMap;
    this.coder = coder;
  }

    @Override
    public Object decode(InputStream inputStream) throws IOException {
      final DataInputStream dis = new DataInputStream(inputStream);
      final char type = dis.readChar();

      switch (type) {
        case KafkaOffloadingOutputEncoder.OFFLOADING_RESULT: {
          final int length = dis.readInt();
          final long watermark = dis.readLong();
          //System.out.println("Decoding " + length + " events");
          final List<Triple<List<String>, String, Object>> data = new ArrayList<>(length);
          for (int i = 0; i < length; i++) {
            final int numOfNextVertices = dis.readInt();
            final List<String> nextVertices = new ArrayList<>(numOfNextVertices);
            for (int j = 0; j < numOfNextVertices; j++) {
              nextVertices.add(dis.readUTF());
            }
            final String edgeId = dis.readUTF();
            final Serializer serializer = serializerMap.get(edgeId);
            final Object object = serializer.getDecoderFactory().create(dis).decode();
            data.add(new Triple<>(nextVertices, edgeId, object));
          }
          return new OffloadingResultEvent(data, watermark);
        }
        case KafkaOffloadingOutputEncoder.KAFKA_CHECKPOINT: {
          return coder.decode(inputStream);
        }
        default: {
          throw new RuntimeException("Unsupported type: " + type);
        }
      }
    }
  }
