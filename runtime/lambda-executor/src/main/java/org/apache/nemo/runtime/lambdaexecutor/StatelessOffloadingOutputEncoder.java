package org.apache.nemo.runtime.lambdaexecutor;

import org.apache.nemo.offloading.common.OffloadingEncoder;
import org.apache.nemo.runtime.executor.common.Serializer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public final class StatelessOffloadingOutputEncoder implements OffloadingEncoder<OffloadingResultEvent> {

  final Map<String, Serializer> serializerMap;

  public StatelessOffloadingOutputEncoder(final Map<String, Serializer> serializerMap) {
    this.serializerMap = serializerMap;
  }

    @Override
    public void encode(OffloadingResultEvent element, OutputStream outputStream) throws IOException {
      final DataOutputStream dos = new DataOutputStream(outputStream);
      dos.writeInt(element.data.size());
      //System.out.println("Encoding " + element.data.size() + " events");

      for (final Triple<List<String>, String, Object> triple : element.data) {
        // vertex id
        dos.writeInt(triple.first.size());
        for (final String nextVertexId : triple.first) {
          dos.writeUTF(nextVertexId);
        }
        // edge id
        dos.writeUTF(triple.second);
        final Serializer serializer = serializerMap.get(triple.second);
        serializer.getEncoderFactory().create(outputStream).encode(triple.third);
      }
    }
  }
