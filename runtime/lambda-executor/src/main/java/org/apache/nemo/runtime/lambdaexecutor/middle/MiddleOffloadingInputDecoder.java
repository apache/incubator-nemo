package org.apache.nemo.runtime.lambdaexecutor.middle;

import org.apache.nemo.common.Pair;
import org.apache.nemo.offloading.common.OffloadingDecoder;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingDataEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class MiddleOffloadingInputDecoder implements OffloadingDecoder<Object> {
  private static final Logger LOG = LoggerFactory.getLogger(MiddleOffloadingInputDecoder.class.getName());
  final Map<String, Serializer> serializerMap;

  public MiddleOffloadingInputDecoder(final Map<String, Serializer> serializerMap) {
    this.serializerMap = serializerMap;
  }

    @Override
    public Object decode(InputStream inputStream) throws IOException {
      final DataInputStream dis = new DataInputStream(inputStream);
      final boolean isControl = dis.readBoolean();

      if (!isControl) {
        final long watermark = dis.readLong();
        //System.out.println("Decoding " + length + " inputs");
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
        return new MiddleOffloadingDataEvent(Pair.of(nextVerticeIds, object), watermark);
      } else {
        final int taskIndex = (int) dis.readLong();
        return new MiddleOffloadingPrepEvent(taskIndex);
      }
    }
  }
