package com.ctrip.framework.drc.core.driver.command.netty;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.NettyClientWithEndpoint;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.netty.commands.ByteBufReceiver;
import com.ctrip.xpipe.netty.commands.DefaultNettyClient;
import com.ctrip.xpipe.utils.ChannelUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * Created by mingdongli
 * 2019/9/9 下午8:13.
 */
public class AsyncNettyClientWithEndpoint extends DefaultNettyClient implements NettyClientWithEndpoint {

    private Logger logger = LoggerFactory.getLogger(AsyncNettyClientWithEndpoint.class);

    private ChannelFuture future;

    private Endpoint endpoint;

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    public AsyncNettyClientWithEndpoint(ChannelFuture future, Endpoint endpoint) {
        super(future.channel());
        this.future = future;
        this.endpoint = endpoint;
        future.addListener((ChannelFutureListener) f -> {
            if(f.isSuccess()) {
                logger.info("[async][connected] endpint: {}, channel: {}", endpoint, ChannelUtil.getDesc(f.channel()));
            } else {
                Throwable throwable = f.cause();
                if (throwable != null) {
                    logger.error("[connect] error", throwable);
                }
            }
        });
    }

    @Override
    public void sendRequest(ByteBuf byteBuf) {
        if(future.channel() != null && future.channel().isActive()) {
            super.sendRequest(byteBuf);
        } else {
            future.addListener((ChannelFutureListener) future -> {
                if(future.isSuccess()) {
                    logger.info("[async][send][{}]", desc);
                    AsyncNettyClientWithEndpoint.super.sendRequest(byteBuf);
                } else {
                    logger.warn("[async][wont-send][{}]", desc);
                }
            });
        }
    }

    @Override
    public void sendRequest(ByteBuf byteBuf, ByteBufReceiver byteBufReceiver) {
        if(future.channel() != null && future.channel().isActive()) {
            AsyncNettyClientWithEndpoint.super.sendRequest(byteBuf, byteBufReceiver);
        } else {
            future.addListener((ChannelFutureListener) future -> {
                if(future.isSuccess()) {
                    logger.info("[async][send][{}] {}", desc, byteBufReceiver.getClass().getSimpleName());
                    AsyncNettyClientWithEndpoint.super.sendRequest(byteBuf, byteBufReceiver);
                } else {
                    logger.warn("[async][wont-send][{}] {}", desc, byteBufReceiver.getClass().getSimpleName());
                }
            });
        }
    }

    @Override
    public Endpoint endpoint() {
        return endpoint;
    }

    @Override
    public CountDownLatch getCountDownLatch() {
        return countDownLatch;
    }

    public ChannelFuture getFuture() {
        return future;
    }
}
