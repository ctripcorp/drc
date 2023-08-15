package com.ctrip.framework.drc.core.driver.command.netty.codec;

import com.ctrip.framework.drc.core.config.DynamicConfig;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONNECTION_IDLE_TIMEOUT_SECOND;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.HEARTBEAT_LOGGER;

/**
 * Created by jixinwang on 2023/8/8
 */
public class ReceiveCheckHandler extends ChannelInboundHandlerAdapter {

    private static int CHECK_PERIOD = CONNECTION_IDLE_TIMEOUT_SECOND * 1000 * 2;

    private boolean msgReceived = true;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        msgReceived = true;
        ctx.fireChannelRead(msg);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ctx.executor().scheduleWithFixedDelay(() -> {
            boolean checkSwitch = DynamicConfig.getInstance().getReceiveCheckSwitch();
            if (!msgReceived && checkSwitch) {
                msgReceived = true;
                HEARTBEAT_LOGGER.info("[ReceiveCheck] receive check error, close channel");
                ctx.close();
            } else {
                HEARTBEAT_LOGGER.info("[ReceiveCheck] receive check success");
                msgReceived = false;
            }
            msgReceived = false;
        }, CHECK_PERIOD, CHECK_PERIOD, TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    protected void setCheckPeriod(int checkPeriod) {
        CHECK_PERIOD = checkPeriod;
    }
}
