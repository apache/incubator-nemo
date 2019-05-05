package org.apache.nemo.runtime.lambdaexecutor.downstream;

import org.apache.nemo.common.Pair;
import org.apache.nemo.offloading.common.OffloadingDecoder;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.lambdaexecutor.middle.MiddleOffloadingDataEvent;
import org.apache.nemo.runtime.lambdaexecutor.middle.MiddleOffloadingPrepEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class DownstreamOffloadingInputDecoder implements OffloadingDecoder<Object> {
  private static final Logger LOG = LoggerFactory.getLogger(DownstreamOffloadingInputDecoder.class.getName());
  final Map<String, Serializer> serializerMap;

  public DownstreamOffloadingInputDecoder(final Map<String, Serializer> serializerMap) {
    this.serializerMap = serializerMap;
  }

    @Override
    public Object decode(InputStream inputStream) throws IOException {
      final DataInputStream dis = new DataInputStream(inputStream);
      final boolean isControl = dis.readBoolean();

      if (!isControl) {
        final String edgeId = dis.readUTF();
        final String opId = dis.readUTF();
        final Serializer serializer = serializerMap.get(edgeId);
        final Object object = serializer.getDecoderFactory().create(dis).decode();
        //System.out.println("Decoded data " + vertexId + "/" + edgeId + " cnt: " + i);
        return new DownstreamOffloadingDataEvent(object, opId);
      } else {
        final int taskIndex = (int) dis.readLong();
        return new DownstreamOffloadingPrepEvent(taskIndex);
      }
    }
  }
