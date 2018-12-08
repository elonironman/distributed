package com.chendaping.common;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Created by chendaping on 2018/12/8.
 */
public class NettyEncoder extends MessageToByteEncoder<Command> {
    @Override
    public void encode(ChannelHandlerContext ctx, Command cmd, ByteBuf out) {
        try {
            int length = cmd.length();
            out.writeInt(length);
            out.writeInt(cmd.getType());
            out.writeLong(cmd.getRequestId());
            out.writeBytes(cmd.getBody());
        } catch (Exception e) {
            ctx.channel().close();
        }
    }
}
