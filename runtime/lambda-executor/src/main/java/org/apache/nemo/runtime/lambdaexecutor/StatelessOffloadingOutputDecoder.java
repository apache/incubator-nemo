package org.apache.nemo.runtime.lambdaexecutor;

import org.apache.nemo.offloading.common.OffloadingDecoder;
import org.apache.nemo.runtime.executor.common.Serializer;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class StatelessOffloadingOutputDecoder implements OffloadingDecoder<OffloadingResultEvent> {

  final Map<String, Serializer> serializerMap;

  public StatelessOffloadingOutputDecoder(final Map<String, Serializer> serializerMap) {
    this.serializerMap = serializerMap;
  }

    @Override
    public OffloadingResultEvent decode(InputStream inputStream) throws IOException {
      final DataInputStream dis = new DataInputStream(inputStream);
      final int length = dis.readInt();
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
      return new OffloadingResultEvent(data);
    }
  }
