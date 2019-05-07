package org.apache.nemo.runtime.lambdaexecutor.middle;

import org.apache.nemo.offloading.common.OffloadingEncoder;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingHeartbeatEvent;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultTimestampEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutputEncoder.HEARTBEAT;
import static org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutputEncoder.OFFLOADING_RESULT;

public final class MiddleOffloadingOutputEncoder implements OffloadingEncoder<Object> {
  private static final Logger LOG = LoggerFactory.getLogger(MiddleOffloadingOutputEncoder.class.getName());

  public MiddleOffloadingOutputEncoder() {
  }

  @Override
  public void encode(Object data, OutputStream outputStream) throws IOException {

    if (data instanceof OffloadingResultTimestampEvent) {
      final OffloadingResultTimestampEvent element = (OffloadingResultTimestampEvent) data;
      //LOG.info("Encode elment: {}, {}, {}", element.vertexId, element.timestamp, element.watermark);
      final DataOutputStream dos = new DataOutputStream(outputStream);
      dos.writeChar(OFFLOADING_RESULT);
      dos.writeUTF(element.vertexId);
      dos.writeLong(element.timestamp);
      dos.writeLong(element.watermark);
    } else if (data instanceof OffloadingHeartbeatEvent) {
      final DataOutputStream dos = new DataOutputStream(outputStream);
      final OffloadingHeartbeatEvent element = (OffloadingHeartbeatEvent) data;
      dos.writeChar(HEARTBEAT);
      dos.writeInt(element.taskIndex);
      dos.writeLong(element.time);
    }
  }
}
