package org.apache.nemo.runtime.lambdaexecutor.datatransfer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.nemo.common.RendevousResponse;
import org.apache.nemo.common.RendevousMessageEncoder;

import java.io.IOException;
import java.util.List;

public final class RendevousClientDecoder extends MessageToMessageDecoder<ByteBuf> {

  private final RendevousServerClient client;

  public RendevousClientDecoder(final RendevousServerClient client) {
    this.client = client;
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {

  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf byteBuf, List<Object> out) throws IOException {

    final RendevousMessageEncoder.Type type = RendevousMessageEncoder.Type.values()[byteBuf.readInt()];
    final ByteBufInputStream bis = new ByteBufInputStream(byteBuf);

    switch (type) {
      case RESPONSE:
        final String dst = bis.readUTF();
        final String address = bis.readUTF();
        client.registerResponse(new RendevousResponse(dst, address));
        break;
      case REQUEST:
        throw new RuntimeException("Unsupported");
      default:
        throw new RuntimeException("Unsupported");
    }

  }
}
