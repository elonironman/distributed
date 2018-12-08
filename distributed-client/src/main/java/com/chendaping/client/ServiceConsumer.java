package com.chendaping.client;
import com.alibaba.fastjson.JSON;
import com.chendaping.common.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
/**
 * Created by chendaping on 2018/12/8.
 */
public class ServiceConsumer implements InvocationHandler {
    private final Bootstrap bootstrap = new Bootstrap();
    private final Bootstrap registry = new Bootstrap();

    // <ip:port, channel>，本例子只支持一个接口。生产环境需要支持多服务（interface）
    private final ConcurrentHashMap<String, Channel> channelTables = new ConcurrentHashMap<String, Channel>();
    // 存放调用响应
    private final ConcurrentHashMap<Long, Command> responses = new ConcurrentHashMap<Long, Command>();
    private final Timer timer = new Timer("registry-heartbeat", true);

    private final EventLoopGroup eventLoopGroupWorker;
    private final EventLoopGroup eventLoopGroupRegistry;

    // 远端服务接口
    private Class interfaceClass;

    public ServiceConsumer(Class interfaceClass) {
        this.interfaceClass = interfaceClass;
        // 服务连接线程组
        eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyClientWorker_%d", this.threadIndex.incrementAndGet()));
            }
        });
        // 注册中心连接线程组
        eventLoopGroupRegistry = new NioEventLoopGroup(1, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyClientRegistry_%d", this.threadIndex.incrementAndGet()));
            }
        });
    }

    /**
     * 这里简单的用代理就可以了。
     * 生产环境：需要用javassist，jdk等字节码技术动态生产class
     */
    public <T> T getTarget() {
        final Class<?>[] interfaces = new Class[]{interfaceClass};
        return (T) Proxy.newProxyInstance(ServiceConsumer.class.getClassLoader(), interfaces, this);
    }

    public void start() {
        // 定义远端服务连接参数
        bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class)//
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(
                                new NettyEncoder(),
                                new NettyDecoder(),
                                new IdleStateHandler(0, 0, 6), //
                                new NettyClientHandler());
                    }
                });
        // 定义注册中心连接参数
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
                                new NettyClientHandler());
                    }
                });

        // 定期从注册中心拉取服务端ip，port
        timer.scheduleAtFixedRate(new TimerTask() {
            volatile boolean registryOK = false;
            volatile Channel channel;

            @Override
            public void run() {
                try {
                    while (!registryOK) {
                        // 注册中心 port 8200
                        ChannelFuture channelFuture = registry.connect(new InetSocketAddress("127.0.0.1", 8200));
                        channelFuture.syncUninterruptibly().addListener(new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture channelFuture) throws Exception {
                                registryOK = true;
                                channel = channelFuture.channel();
                            }
                        });
                    }

                    if (registryOK && channel.isActive()) {
                        byte[] body = interfaceClass.getCanonicalName().getBytes("UTF-8");
                        Command cmd = new Command(Command.GET_SERVER_LIST, body);
                        channel.writeAndFlush(cmd);
                    } else {
                        channel.close();
                        registryOK = false;
                    }
                } catch (Exception e) {
                    registryOK = false;
                }
            }
        }, 10, 1000);
    }

    class NettyClientHandler extends SimpleChannelInboundHandler<Command> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Command msg) throws Exception {
            final Command cmd = msg;
            if (cmd != null) {
                switch (cmd.getType()) {
                    case Command.INVOKE_RESPONSE:
                        responses.put(msg.getRequestId(), msg);
                        break;
                    case Command.GET_SERVER_LIST_RESPONSE:
                        try {
                            String str = new String(msg.getBody(), "UTF-8");
                            String[] servers = str.split(",");
                            for (String ip : servers) {
                                System.out.println("服务提供者：" + ip);
                                String[] ipAndPort = ip.split(":");
                                // 已经连接到了服务端，跳过
                                if (channelTables.containsKey(ip)) {
                                    continue;
                                }
                                if (ipAndPort.length == 2) {
                                    Channel channel = bootstrap.connect(new InetSocketAddress(ipAndPort[0], Integer.parseInt(ipAndPort[1])))
                                            .sync().channel();
                                    channelTables.put(ip, channel);
                                }
                            }
                        }catch (Exception e){}
                        break;
                    default:
                        break;
                }
            }
        }// channelRead0
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Invocation inv = new Invocation();
        inv.setInterfaceName(interfaceClass.getCanonicalName());
        inv.setMethodName(method.getName());
        inv.setArguments(args);

        String[] types = new String[method.getParameterTypes().length];
        int index = 0;
        for (Class type : method.getParameterTypes()) {
            types[index] = type.getCanonicalName();
            index++;
        }
        inv.setParameterTypes(types);

        Command cmd = new Command(Command.INVOKE_REQUEST, JSON.toJSONBytes(inv));
        // 如果服务端没有就位，等待。这里只是例子，生产环境需要用超时和线程池。
        while(channelTables.isEmpty()) {
            Thread.sleep(2);
        }
        for (String key : channelTables.keySet()) {
            channelTables.get(key).writeAndFlush(cmd);
            System.out.println("目标调用服务器：" + channelTables.get(key).toString());
        }

        // 等待服务端返回。生产环境需要用Future来控制，这里为了简单。
        try {
            Thread.sleep(5);
        } catch (Exception e){}

        Command response = responses.get(cmd.getRequestId());
        if (response != null) {
            return JSON.parse(response.getBody());
        }
        return null;
    }

    public static void main(String[] args) throws Exception{
        ServiceConsumer sc = new ServiceConsumer(IService.class);
        sc.start();
        IService service = sc.getTarget();
        System.out.println("IService.hello(\"world\"): " + service.hello("world"));
    }
}
