package org.apache.nemo.runtime.lambdaexecutor;

import org.apache.nemo.common.Pair;
import org.apache.nemo.offloading.common.OffloadingDecoder;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class StatelessOffloadingInputDecoder implements OffloadingDecoder<OffloadingDataEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(StatelessOffloadingInputDecoder.class.getName());
  final Map<String, Serializer> serializerMap;

  public StatelessOffloadingInputDecoder(final Map<String, Serializer> serializerMap) {
    this.serializerMap = serializerMap;
  }

    @Override
    public OffloadingDataEvent decode(InputStream inputStream) throws IOException {
      final DataInputStream dis = new DataInputStream(inputStream);
      final int length = dis.readInt();
      final long watermark = dis.readLong();

      //System.out.println("Decoding " + length + " inputs");
      final List<Pair<List<String>, Object>> data = new ArrayList<>(length);
      for (int i = 0; i < length; i++) {
        final int nextVertices = dis.readInt();
        final List<String> nextVerticeIds = new ArrayList<>(nextVertices);
        for (int j = 0; j < nextVertices; j++) {
          final String nextVertexId = dis.readUTF();
          nextVerticeIds.add(nextVertexId);
        }
        final String edgeId = dis.readUTF();
        final Serializer serializer = serializerMap.get(edgeId);
        final Object object = serializer.getDecoderFactory().create(dis).decode();
        //System.out.println("Decoded data " + vertexId + "/" + edgeId + " cnt: " + i);
        data.add(Pair.of(nextVerticeIds, object));
      }
      return new OffloadingDataEvent(data, watermark);
    }
  }
