package com.ctrip.framework.drc.core.driver.command.netty.codec;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jixinwang on 2023/8/8
 */
public class ReceiveCheckHandler extends ChannelInboundHandlerAdapter {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private FileCheck fileCheck;

    private boolean readIdle;

    public ReceiveCheckHandler(FileCheck fileCheck) {
        this.fileCheck = fileCheck;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("channel active: " + ctx.channel().toString());
        fileCheck.setChannel(ctx.channel());
        fileCheck.startCheck();
        ctx.fireChannelActive();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        fileCheck.stopCheck();
        ctx.fireChannelInactive();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            if (e.state() == IdleState.READER_IDLE) {
                readIdle = true;
            }
        }
        ctx.fireUserEventTriggered(evt);
    }
}
