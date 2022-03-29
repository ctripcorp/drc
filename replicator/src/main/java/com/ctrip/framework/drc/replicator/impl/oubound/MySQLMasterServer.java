package com.ctrip.framework.drc.replicator.impl.oubound;

import com.ctrip.framework.drc.core.driver.command.handler.CommandHandler;
import com.ctrip.framework.drc.core.driver.command.netty.codec.UnpackDecoder;
import com.ctrip.framework.drc.core.server.config.replicator.MySQLMasterConfig;
import com.ctrip.framework.drc.replicator.MySQLServer;
import com.ctrip.framework.drc.replicator.impl.AbstractMySQLServer;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.CommandHandlerManager;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.ReplicatorMasterHandler;
import com.ctrip.xpipe.concurrent.NamedThreadFactory;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.commons.lang3.StringUtils;

import java.net.InetSocketAddress;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.MASTER_HEARTBEAT_PERIOD_SECONDS;

/**
 * Created by mingdongli
 * 2019/9/21 下午9:20
 */
public class MySQLMasterServer extends AbstractMySQLServer implements MySQLServer {

    private static final int WORK_THREAD_COUNT = 1;

    private static final int RECV_BUFFER_SIZE = 4096;

    private static final int MEGA_BYTE = 1000 * 1000;

    public static final int WRITE_LOW_WATER_MARK = 5 * MEGA_BYTE;

    public static final int WRITE_HIGH_WATER_MARK = 10 * WRITE_LOW_WATER_MARK;

    private MySQLMasterConfig mySQLMasterConfig;

    private EventLoopGroup bossGroup ;

    private EventLoopGroup workerGroup;

    private ServerBootstrap bootstrap;

    private ServerSocketChannel serverChannel;

    private CommandHandlerManager handlerManager;

    public MySQLMasterServer(MySQLMasterConfig mySQLMasterConfig) {
        this.mySQLMasterConfig = mySQLMasterConfig;
        handlerManager = new CommandHandlerManager();
    }

    protected void doInitialize() throws Exception {
        super.doInitialize();
        LifecycleHelper.initializeIfPossible(handlerManager);
        bootstrap = new ServerBootstrap();
        bossGroup = new NioEventLoopGroup(1, new NamedThreadFactory("Binlog-Acceptor-" + mySQLMasterConfig.getPort()));
        workerGroup = new NioEventLoopGroup(WORK_THREAD_COUNT, new NamedThreadFactory("Binlog-Sender-" + mySQLMasterConfig.getPort()));
    }

    protected void doStart() throws Exception {
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(WRITE_LOW_WATER_MARK, WRITE_HIGH_WATER_MARK))
                .childOption(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(RECV_BUFFER_SIZE))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new LoggingHandler(LogLevel.DEBUG));
                        p.addLast(new UnpackDecoder());
                        p.addLast(new ChunkedWriteHandler());
                        p.addLast(new IdleStateHandler(0, MASTER_HEARTBEAT_PERIOD_SECONDS, 0));
                        p.addLast(new ReplicatorMasterHandler(handlerManager));
                    }
                });

        if (StringUtils.isNotEmpty(mySQLMasterConfig.getIp())) {
            this.serverChannel = (ServerSocketChannel) bootstrap.bind(new InetSocketAddress(mySQLMasterConfig.getIp(), mySQLMasterConfig.getPort())).sync().channel();
        } else {
            this.serverChannel = (ServerSocketChannel) bootstrap.bind(new InetSocketAddress(mySQLMasterConfig.getPort())).sync().channel();
        }
    }

    protected void doDispose() throws Exception {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        serverChannel.close();
        LifecycleHelper.disposeIfPossible(handlerManager);
    }

    @Override
    public void addCommandHandler(CommandHandler handler) {
        handlerManager.addHandler(handler);
    }

}
