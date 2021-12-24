package com.ctrip.framework.drc.console.monitor.netty;

import com.ctrip.framework.drc.console.monitor.netty.handler.HeartBeatServerHandler;
import com.ctrip.framework.drc.core.driver.command.netty.codec.*;
import com.ctrip.framework.drc.core.server.config.replicator.MySQLMasterConfig;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.concurrent.NamedThreadFactory;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-03-20
 * A Netty Server mainly used for unit test, say act as a replicator
 */
public class NettyServer extends AbstractLifecycle implements Lifecycle {

    private static final int BOSS_THREAD_COUNT = 1;

    private static final int WORKER_THREAD_COUNT = 4;

    private EventLoopGroup bossGroup;

    private EventLoopGroup workergroup;

    private ServerBootstrap serverBootstrap;

    private MySQLMasterConfig mySQLMasterConfig;

    private ServerSocketChannel serverSocketChannel;

    public NettyServer(MySQLMasterConfig mySQLMasterConfig) {
        this.mySQLMasterConfig = mySQLMasterConfig;
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        serverBootstrap = new ServerBootstrap();
        bossGroup = new NioEventLoopGroup(BOSS_THREAD_COUNT, new NamedThreadFactory("Server-Master"));
        workergroup = new NioEventLoopGroup(WORKER_THREAD_COUNT, new NamedThreadFactory("Server-Workers"));
    }

    @Override
    protected void doStart() throws Exception {

        serverBootstrap.group(bossGroup, workergroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast("ping", new IdleStateHandler(0, 1, 0));
                        p.addLast(new PackageEncoder());
                        p.addLast(new HeartBeatServerHandler());
                    }
                });

        serverSocketChannel = (ServerSocketChannel) serverBootstrap.bind(mySQLMasterConfig.getPort()).sync().channel();
    }

    @Override
    protected void doDispose() {
        // order and shutdownGracefully
        bossGroup.shutdownGracefully();
        workergroup.shutdownGracefully();
        serverSocketChannel.close();
    }
}
