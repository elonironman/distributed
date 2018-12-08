package com.chendaping.common;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import java.nio.ByteBuffer;

/**
 * Created by chendaping on 2018/12/8.
 */
public class NettyDecoder extends LengthFieldBasedFrameDecoder {

    public NettyDecoder() {
        super(65536, 0, 4, 0, 4);
    }

    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = null;
        try {
            frame = (ByteBuf) super.decode(ctx, in);
            if (null == frame) {
                return null;
            }

            ByteBuffer byteBuffer = frame.nioBuffer();
            int length = byteBuffer.limit();
            int type = byteBuffer.getInt();
            long requestId = byteBuffer.getLong();
            byte[] bodyData = null;
            if ((length - 12) > 0) {
                bodyData = new byte[length - 12];
                byteBuffer.get(bodyData);
            }
            Command cmd = new Command(type, bodyData);
            cmd.setRequestId(requestId);
            return cmd;
        } catch (Exception e) {
            ctx.channel().close().addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    System.out.println("closeChannel");
                }
            });
        } finally {
            if (null != frame) {
                frame.release();
            }
        }
        return null;
    }
}