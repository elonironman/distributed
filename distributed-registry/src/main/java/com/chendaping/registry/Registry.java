package com.chendaping.registry;
import com.chendaping.common.NettyDecoder;
import com.chendaping.common.NettyEncoder;
import com.chendaping.common.Command;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
/**
 * Created by chendaping on 2018/12/8.
 */
public class Registry {
    private final ServerBootstrap serverBootstrap;
    private final EventLoopGroup eventLoopGroupWorker;
    private final EventLoopGroup eventLoopGroupBoss;

    // 存放对外提供的服务对象 <interface, servers>
    private final ConcurrentHashMap<String, List<String>> servers = new ConcurrentHashMap<String, List<String>>();

    public Registry() {
        this.serverBootstrap = new ServerBootstrap();

        this.eventLoopGroupBoss = new NioEventLoopGroup(1, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyBoss_%d", this.threadIndex.incrementAndGet()));
            }
        });

        final int threadCount = Runtime.getRuntime().availableProcessors() * 2;
        this.eventLoopGroupWorker = new NioEventLoopGroup(threadCount, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);
            private int threadTotal = threadCount;
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyServerWorker_%d_%d", threadTotal, this.threadIndex.incrementAndGet()));
            }
        });
    }

    public void start() {
        serverBootstrap.group(eventLoopGroupBoss, eventLoopGroupWorker)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024) // 链接队列个数
                .childOption(ChannelOption.TCP_NODELAY, true)
                .localAddress("127.0.0.1", 8200)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(
                                new NettyEncoder(),
                                new NettyDecoder(),
                                new IdleStateHandler(0, 0, 60), //空闲链路状态处理
                                new NettyServerHandler());
                    }
                });

        try {
            serverBootstrap.bind().sync().addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    System.out.println("registry 创建成功");
                }
            });
        } catch (InterruptedException e1) {
            throw new RuntimeException("serverBootstrap.bind().sync() InterruptedException", e1);
        }
    }

    class NettyServerHandler extends SimpleChannelInboundHandler<Command> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Command msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }

    // 根据不同的命令，相应处理
    public void processMessageReceived(ChannelHandlerContext ctx, Command msg) {
        final Command cmd = msg;
        if (cmd != null) {
            switch (cmd.getType()) {
                case Command.GET_SERVER_LIST:
                    try {
                        String interfaceName = new String(msg.getBody(), "UTF-8");
                        List<String> ips = servers.get(interfaceName);
                        if (ips == null) {
                            ips = new ArrayList<String>();
                            servers.put(interfaceName, ips);
                        }
                        // 格式： ip:port,ip:port
                        String str = StringUtils.join(ips, ",");
                        byte[] body = String.valueOf(str).getBytes("UTF-8");
                        Command response = new Command(Command.GET_SERVER_LIST_RESPONSE, body);
                        response.setRequestId(msg.getRequestId());
                        ctx.channel().writeAndFlush(response);
                    } catch (Exception e) {}
                    break;
                case Command.REGISTER_SERVER:
                case Command.UNREGISTER_SERVER:
                    // 格式：interface,ip:port
                    try {
                        String str = new String(msg.getBody(), "UTF-8");
                        System.out.println("服务注册：" + str);
                        String[] aStr = str.split(",");
                        List<String> ips = servers.get(aStr[0]);
                        if (ips == null) {
                            ips = new ArrayList<String>();
                            servers.put(aStr[0], ips);
                        }
                        if (msg.getType() == Command.REGISTER_SERVER && !ips.contains(aStr[1])) {
                            ips.add(aStr[1]);
                        } else {
                            ips.remove(aStr[1]);
                        }
                    } catch (Exception e){
                        System.out.println("error" + e.getMessage());
                    };
                    break;
                default:
                    break;
            }
        }
    }

    public static void main(String[] args) {
        Registry registry = new Registry();
        registry.start();
    }
}
