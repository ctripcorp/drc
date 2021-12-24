package com.ctrip.framework.drc.console.utils;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.concurrent.TimeUnit;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-03-23
 */
@ChannelHandler.Sharable
public class HeartbeatRequestHandler extends IdleStateHandler {

    public long testLastReadTime;

    public HeartbeatRequestHandler(int readerIdleTimeSeconds, int writerIdleTimeSeconds, int allIdleTimeSeconds) {
        super(readerIdleTimeSeconds, writerIdleTimeSeconds, allIdleTimeSeconds);
    }

    public HeartbeatRequestHandler(long readerIdleTime, long writerIdleTime, long allIdleTime, TimeUnit unit) {
        super(readerIdleTime, writerIdleTime, allIdleTime, unit);
    }

    public HeartbeatRequestHandler(boolean observeOutput, long readerIdleTime, long writerIdleTime, long allIdleTime, TimeUnit unit) {
        super(observeOutput, readerIdleTime, writerIdleTime, allIdleTime, unit);
    }

    private static class HeartbeatRequestHandlerHolder {
        public static final HeartbeatRequestHandler INSTANCE = new HeartbeatRequestHandler(30, 0, 0);
    }

    public static HeartbeatRequestHandler getInstance() {
        return HeartbeatRequestHandlerHolder.INSTANCE;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
        testLastReadTime = System.currentTimeMillis();
    }
}
