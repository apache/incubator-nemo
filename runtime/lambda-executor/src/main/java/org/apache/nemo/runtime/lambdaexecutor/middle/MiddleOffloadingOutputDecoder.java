package org.apache.nemo.runtime.lambdaexecutor.middle;

import org.apache.nemo.offloading.common.OffloadingDecoder;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultEvent;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultTimestampEvent;
import org.apache.nemo.runtime.lambdaexecutor.Triple;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOperatorVertexOutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class MiddleOffloadingOutputDecoder implements OffloadingDecoder<OffloadingResultTimestampEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(MiddleOffloadingOutputDecoder.class.getName());

  public MiddleOffloadingOutputDecoder() {
  }

    @Override
    public OffloadingResultTimestampEvent decode(InputStream inputStream) throws IOException {
      final DataInputStream dis = new DataInputStream(inputStream);
      final String vertexId = dis.readUTF();
      final long timestamp = dis.readLong();
      final long watermark = dis.readLong();

      LOG.info("Decode element: {}, {}, {}", vertexId, timestamp, watermark);

      return new OffloadingResultTimestampEvent(vertexId, timestamp, watermark);
    }
  }
