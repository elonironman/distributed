package com.chendaping.server;
import com.alibaba.fastjson.JSON;
import com.chendaping.common.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by chendaping on 2018/12/8.
 */
public class ServiceProvider {
    // 提供服务
    private final ServerBootstrap serverBootstrap;
    // 链接注册中心
    private final Bootstrap registry = new Bootstrap();

    private final EventLoopGroup eventLoopGroupWorker;
    private final EventLoopGroup eventLoopGroupBoss;
    private final EventLoopGroup eventLoopGroupRegistry;

    // 存放对外提供的服务对象 <interface, implement>
    private final Map<String, Object> services = new HashMap<String, Object>();

    // 向注册中心，实时发送心跳
    private final Timer timer = new Timer("registry-heartbeat", true);

    public ServiceProvider() {
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

        eventLoopGroupRegistry = new NioEventLoopGroup(1, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyClientRegistry_%d", this.threadIndex.incrementAndGet()));
            }
        });
    }

    public void addService(String interfaceName, Object service) {
        services.put(interfaceName, service);
    }

    public void start() {
        serverBootstrap.group(eventLoopGroupBoss, eventLoopGroupWorker)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024) // 链接队列个数
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, false) //设置心跳参数 FALSE为不启用参数
                .childOption(ChannelOption.TCP_NODELAY, true)
                .localAddress(new InetSocketAddress(8080))
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
        registry.group(this.eventLoopGroupRegistry)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(
                                new NettyEncoder(),
                                new NettyDecoder(),
                                new IdleStateHandler(0, 0, 60),
                                new NettyServerHandler());
                    }
                });

        try {
            serverBootstrap.bind().sync();
        } catch (InterruptedException e1) {
            throw new RuntimeException("this.serverBootstrap.bind().sync() InterruptedException", e1);
        }

        // 向注册中心，注册自己
        timer.scheduleAtFixedRate(new TimerTask() {
            private volatile boolean registryOK = false;
            private volatile Channel channel;
            @Override
            public void run() {
                try {
                    while (!registryOK) {
                        // 注册中心 port 8200
                        InetSocketAddress registryAddress = new InetSocketAddress("127.0.0.1", 8200);
                        ChannelFuture channelFuture = registry.connect(registryAddress);

                        channelFuture.syncUninterruptibly().addListener(new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture channelFuture) throws Exception {
                                registryOK = true;
                                channel = channelFuture.channel();
                            }
                        });
                    }

                    if (registryOK && channel.isActive()) {
                        for (String key : services.keySet()) {
                            // 服务port和ip
                            byte[] body = (key + ",127.0.0.1:8080").getBytes("UTF-8");
                            Command cmd = new Command(Command.REGISTER_SERVER, body);
                            channel.writeAndFlush(cmd);
                            System.out.println("注册服务 > " + channel.toString());
                        }
                    } else {
                        registryOK = false;
                        channel.close();
                    }
                } catch (Exception e) {
                    registryOK = false;
                    if (null != channel) {
                        channel.close();
                    }
                }
            }
        }, 10, 1000);
    }

    public void shutdown() {
        eventLoopGroupBoss.shutdownGracefully();
        eventLoopGroupWorker.shutdownGracefully();
    }

    class NettyServerHandler extends SimpleChannelInboundHandler<Command> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Command msg) throws Exception {
            final Command cmd = msg;
            switch (cmd.getType()) {
                case Command.INVOKE_REQUEST:
                    try {
                        Invocation invoke = JSON.parseObject(cmd.getBody(), Invocation.class);
                        // 找到服务
                        Object service = services.get(invoke.getInterfaceName());
                        Class cls = Class.forName(invoke.getInterfaceName());
                        List<Class> argsTypeList = new ArrayList<Class>(invoke.getParameterTypes().length);
                        for (String s : invoke.getParameterTypes()) {
                            argsTypeList.add(Class.forName(s));
                        }
                        Method method = cls.getMethod(invoke.getMethodName(), argsTypeList.toArray(new Class[argsTypeList.size()]));
                        Object result = method.invoke(service, invoke.getArguments());

                        Command response = new Command(Command.INVOKE_RESPONSE, JSON.toJSONBytes(result));
                        response.setRequestId(cmd.getRequestId());
                        ctx.channel().writeAndFlush(response);
                    } catch (Exception e) {}
                    break;
                default:
                    break;
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent evnet = (IdleStateEvent) evt;
                if (evnet.state().equals(IdleState.ALL_IDLE)) {
                    System.out.println("NETTY SERVER PIPELINE: IDLE exception");
                    ctx.channel().close();
                }
            }
            ctx.fireUserEventTriggered(evt);
        }
    }

    public static void main(String[] args) {
        ServiceProvider sp = new ServiceProvider();
        sp.addService(IService.class.getCanonicalName(), new ServiceImpl());
        sp.start();
    }
}