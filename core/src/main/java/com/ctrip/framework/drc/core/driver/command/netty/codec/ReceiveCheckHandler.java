package com.ctrip.framework.drc.core.driver.command.netty.codec;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Created by jixinwang on 2023/8/8
 */
public class ReceiveCheckHandler extends ChannelInboundHandlerAdapter {

    private FileCheck fileCheck;

    public ReceiveCheckHandler(FileCheck fileCheck) {
        this.fileCheck = fileCheck;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        fileCheck.start(ctx.channel());
        ctx.fireChannelActive();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        fileCheck.stop();
        ctx.fireChannelInactive();
    }
}
