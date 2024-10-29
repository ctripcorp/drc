package com.ctrip.framework.drc.core.driver.command.netty;

import com.ctrip.framework.drc.core.driver.command.netty.codec.ChannelHandlerFactory;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.NettyClientWithEndpoint;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.utils.ChannelUtil;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.AttributeKey;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author limingdong
 * @create 2021/4/9
 */
public class NettyClientFactory extends AbstractStartStoppable implements PooledObjectFactory<NettyClient> {

    private static Logger logger = LoggerFactory.getLogger(NettyClientFactory.class);

    public static final AttributeKey<NettyClientWithEndpoint> KEY_CLIENT = AttributeKey.newInstance(NettyClientFactory.class.getSimpleName() + "-MySQL-Client");

    protected int connectTimeoutMilli = 1000;

    private ChannelHandlerFactory handlerFactory;

    private NioEventLoopGroup eventLoopGroup;

    protected Bootstrap b = new Bootstrap();

    private String threadName;

    private boolean autoRead;

    private Endpoint target;

    public NettyClientFactory(Endpoint target, String threadPrefix, boolean autoRead) {
        this.threadName = String.format("ANCF-%s", threadPrefix);
        this.autoRead = autoRead;
        this.target = target;
    }

    @Override
    protected void doStart() throws Exception {
        eventLoopGroup = new NioEventLoopGroup(1, XpipeThreadFactory.create(threadName));
        initBootstrap();
    }

    protected void initBootstrap() {
        b.group(eventLoopGroup).channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMilli)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.RCVBUF_ALLOCATOR, getRecvByteBufAllocator())
                .option(ChannelOption.AUTO_READ, this.autoRead)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .handler(new ChannelInitializer() {
                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        addChannelHandler(pipeline);
                    }
                });
    }

    protected RecvByteBufAllocator getRecvByteBufAllocator() {
        return new FixedRecvByteBufAllocator(4096);
    }

    @Override
    public PooledObject<NettyClient> makeObject() {

        ChannelFuture channelFuture = b.connect(target.getHost(), target.getPort()).awaitUninterruptibly();
        NettyClientWithEndpoint nettyClient = new AsyncNettyClientWithEndpoint(channelFuture, target);
        channelFuture.channel().attr(KEY_CLIENT).set(nettyClient);
        channelFuture.addListener(connFuture -> {
            if (connFuture.isSuccess()) {
                if (!autoRead) {
                    logger.info("[Trigger] auto read for {}", target);
                    ChannelUtil.triggerChannelAutoRead(channelFuture.channel());
                }
            }
        });
        return new DefaultPooledObject<>(nettyClient);
    }

    @Override
    public boolean validateObject(PooledObject<NettyClient> p) {
        Channel channel = p.getObject().channel();
        if (channel == null) {
            return false;
        }
        return channel.isActive();
    }

    public void setHandlerFactory(ChannelHandlerFactory handlerFactory) {
        this.handlerFactory = handlerFactory;
    }

    private void addChannelHandler(ChannelPipeline pipeline) {
        for (ChannelHandler handler : handlerFactory.createChannelHandlers()) {
            pipeline.addLast(handler);
        }
    }

    @Override
    public void destroyObject(PooledObject<NettyClient> p) {

        try {
            logger.info("[destroyObject]{}, {}", target, p.getObject());
            ChannelFuture channelFuture = p.getObject().channel().close();
            channelFuture.addListener((ChannelFutureListener) future -> {
                Throwable throwable = future.cause();
                if (throwable != null) {
                    logger.error("[destroyObject] closeFuture error", throwable);
                }
                if (!future.isSuccess()) {
                    logger.info("[Close] {} fail for {}", target, p.getObject());
                }
                if (future.isSuccess()) {
                    logger.info("[Close] {} success for {}", target, p.getObject());
                }
            });
        } catch (Exception e) {
            logger.error("[destroyObject] error", e);
        }
    }

    @Override
    protected void doStop() {
        eventLoopGroup.shutdownGracefully();
    }

    @Override
    public void activateObject(PooledObject<NettyClient> p) throws Exception {

    }

    @Override
    public void passivateObject(PooledObject<NettyClient> p) throws Exception {

    }
}
