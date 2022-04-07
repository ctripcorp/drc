package com.ctrip.framework.drc.replicator.impl.oubound.handler;

import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.replicator.impl.oubound.channel.ChannelAttributeKey;
import com.ctrip.xpipe.netty.commands.DefaultNettyClient;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.utils.Gate;
import com.ctrip.xpipe.utils.MapUtils;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.ChannelInputShutdownReadComplete;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.Map;

/**
 * Created by mingdongli
 * 2019/9/21 下午11:41
 */
public class ReplicatorMasterHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public static final AttributeKey<ChannelAttributeKey> KEY_CLIENT = AttributeKey.newInstance(ReplicatorMasterHandler.class.getSimpleName() + "-Replicator-Server");

    private Map<Channel, NettyClient> nettyClientMap = Maps.newConcurrentMap();

    private CommandHandlerManager handlerManager;

    public ReplicatorMasterHandler(CommandHandlerManager handlerManager) {
        super();
        this.handlerManager = handlerManager;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        logger.info("[Receive] {} to request binlog", ctx.channel().remoteAddress());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        SocketAddress remoteAddress = ctx.channel().remoteAddress();
        logger.info("[Receive] channel inactive for {}", remoteAddress);
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("[Caught] exception", cause);

        if (ctx.channel().isActive()) {
            ResultCode.UNKNOWN_ERROR.sendResultCode(ctx.channel(), logger);
        }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        Channel channel = ctx.channel();
        String channelString = channel.toString();
        boolean writable = channel.isWritable();
        logger.info("{} channelWritabilityChanged to {}", channelString, writable);
        Gate gate = channel.attr(KEY_CLIENT).get().getGate();
        if (writable) {
            gate.open();
        } else {
            gate.close();
        }
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.network.writability", channelString + ":" + writable);
        ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            switch (e.state()) {
                case WRITER_IDLE:
                case READER_IDLE: // for heartbeat
                    Channel channel = ctx.channel();
                    boolean writable = channel.isWritable();
                    if (writable) {
                        handlerManager.sendHeartBeat(channel);
                    } else {
                        logger.info("write idle false and skip heart beat for {} on {}", ctx.channel().toString(), e.state());
                    }
                    break;
                default:
                    break;
            }
        } else if (evt instanceof ChannelInputShutdownReadComplete) {
            ctx.close();
            logger.info("receive ChannelInputShutdownReadComplete event for {}", ctx.channel().toString());
        } else {
            logger.info("receive {} event for {}", evt.toString(), ctx.channel().toString());
        }
        ctx.fireUserEventTriggered(evt);
    }

    /**
     * applier 订阅命令，replicator slave订阅命令，meta信息同步信息
     * @param ctx
     * @param in
     * @throws Exception
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        if (in != null && in.readableBytes() > 0) {
            NettyClient nettyClient = MapUtils.getOrCreate(nettyClientMap, ctx.channel(),
                    () -> {
                        Channel channel = ctx.channel();
                        DefaultNettyClient defaultNettyClient=  new DefaultNettyClient(channel);
                        channel.closeFuture().addListener((ChannelFutureListener) future -> {
                            NettyClient cache = nettyClientMap.remove(channel);
                            logger.info("[Remove] {}:{} from nettyClientMap", channel, cache);
                        });
                        ctx.channel().attr(KEY_CLIENT).set(new ChannelAttributeKey(new Gate(ctx.channel().remoteAddress().toString())));
                        return defaultNettyClient;
                    });

            handlerManager.handle(nettyClient, in);
        }

    }

}
