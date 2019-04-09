package org.apache.nemo.runtime.lambdaexecutor.middle;

import org.apache.nemo.offloading.common.OffloadingEncoder;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultTimestampEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public final class MiddleOffloadingOutputEncoder implements OffloadingEncoder<OffloadingResultTimestampEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(MiddleOffloadingOutputEncoder.class.getName());

  public MiddleOffloadingOutputEncoder() {
  }

  @Override
  public void encode(OffloadingResultTimestampEvent element, OutputStream outputStream) throws IOException {
    LOG.info("Encode elment: {}, {}, {}", element.vertexId, element.timestamp, element.watermark);
    final DataOutputStream dos = new DataOutputStream(outputStream);
    dos.writeUTF(element.vertexId);
    dos.writeLong(element.timestamp);
    dos.writeLong(element.watermark);
  }
}
