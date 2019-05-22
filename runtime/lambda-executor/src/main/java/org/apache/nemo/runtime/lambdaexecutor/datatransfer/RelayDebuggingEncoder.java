package org.apache.nemo.runtime.lambdaexecutor.datatransfer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.nemo.runtime.executor.common.datatransfer.ByteTransferContextSetupMessage;
import org.apache.nemo.runtime.executor.common.datatransfer.ControlFrameEncoder;
import org.apache.nemo.runtime.executor.common.relayserverclient.RelayControlFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public final class RelayDebuggingEncoder extends MessageToMessageEncoder<ByteTransferContextSetupMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(RelayDebuggingEncoder.class.getName());


  public static final int ZEROS_LENGTH = 5;
  public static final int BODY_LENGTH_LENGTH = Integer.BYTES;
  public static final ByteBuf ZEROS = Unpooled.directBuffer(ZEROS_LENGTH, ZEROS_LENGTH).writeZero(ZEROS_LENGTH);


  public RelayDebuggingEncoder() {
  }

  @Override
  protected void encode(final ChannelHandlerContext ctx, final ByteTransferContextSetupMessage in, final List out) {
    // encode header
    LOG.info("Encoding setup message: {}", in);
    throw new RuntimeException("Not supported!!");
  }
}
