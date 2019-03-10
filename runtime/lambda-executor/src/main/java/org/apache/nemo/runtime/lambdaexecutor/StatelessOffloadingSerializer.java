package org.apache.nemo.runtime.lambdaexecutor;

import org.apache.nemo.common.Pair;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.offloading.common.OffloadingDecoder;
import org.apache.nemo.offloading.common.OffloadingEncoder;
import org.apache.nemo.offloading.common.OffloadingSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StatelessOffloadingSerializer implements OffloadingSerializer {
  private static final Logger LOG = LoggerFactory.getLogger(StatelessOffloadingSerializer.class.getName());
  private final Map<String, Serializer> serializerMap;
  private final OffloadingDecoder inputDecoder = new StatelessOffloadingInputDecoder();
  private final OffloadingEncoder outputEncoder = new StatelessOffloadingOutputEncoder();
  private final OffloadingDecoder outputDecoder = new StatelessOffloadingOutputDecoder();

  public StatelessOffloadingSerializer(final Map<String, Serializer> serializerMap) {
    this.serializerMap = serializerMap;
  }

  @Override
  public OffloadingEncoder getInputEncoder() {
    return null;
  }

  @Override
  public OffloadingDecoder getInputDecoder() {
    return inputDecoder;
  }

  @Override
  public OffloadingEncoder getOutputEncoder() {
    return outputEncoder;
  }

  @Override
  public OffloadingDecoder getOutputDecoder() {
    return outputDecoder;
  }

  public final class StatelessOffloadingInputDecoder implements OffloadingDecoder<OffloadingDataEvent> {

    @Override
    public OffloadingDataEvent decode(InputStream inputStream) throws IOException {
      final DataInputStream dis = new DataInputStream(inputStream);
      final int length = dis.readInt();
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
      return new OffloadingDataEvent(data);
    }
  }

  public final class StatelessOffloadingOutputEncoder implements OffloadingEncoder<OffloadingResultEvent> {

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

  public final class StatelessOffloadingOutputDecoder implements OffloadingDecoder<OffloadingResultEvent> {

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
}
