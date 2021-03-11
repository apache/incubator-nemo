package org.apache.nemo.runtime.message.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class NettyControlMessageCodec {
  private static final Logger LOG = LoggerFactory.getLogger(NettyControlMessageCodec.class.getName());


  public static final class Encoder  extends MessageToMessageEncoder<ControlMessageWrapper> {

    private final boolean isExecutor;

    public Encoder(final boolean isExecutor) {
      this.isExecutor = isExecutor;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ControlMessageWrapper msg, List<Object> out) throws Exception {
      final ByteBufOutputStream bos = new ByteBufOutputStream(ctx.alloc().ioBuffer());
      bos.writeByte(msg.type.ordinal());
      msg.msg.writeTo(bos);
      bos.close();
      out.add(bos.buffer());

      /*
      if (isExecutor) {
        LOG.info("Encoding control message in executor size {} {}, {}, {}",
          bos.buffer().readableBytes(),
          msg.type, msg.msg.getType(), msg.msg.getId());
      } else {
         LOG.info("Encoding control message in master size {} {}, {}, {}",
          bos.buffer().readableBytes(),
          msg.type, msg.msg.getType(), msg.msg.getId());
      }
      */

    }
  }

  public static final class Decoder extends MessageToMessageDecoder<ByteBuf> {

    private final boolean isExecutor;

    public Decoder(final boolean isExecutor) {
      this.isExecutor = isExecutor;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
      /*
      if (!isExecutor) {
        LOG.info("Decoding control message in master size {}", msg.readableBytes());
      } else {
        LOG.info("Decoding control message in executor size {}", msg.readableBytes());
      }
      */
      final ByteBufInputStream bis = new ByteBufInputStream(msg);
      final ControlMessageWrapper.MsgType type = ControlMessageWrapper.MsgType.values()[bis.readByte()];
      final ControlMessage.Message message = ControlMessage.Message.parseFrom(bis);
      bis.close();

      out.add(new ControlMessageWrapper(type, message));
    }
  }
}
