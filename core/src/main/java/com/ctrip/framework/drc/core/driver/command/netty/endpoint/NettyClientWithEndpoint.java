package com.ctrip.framework.drc.core.driver.command.netty.endpoint;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.netty.commands.NettyClient;

import java.util.concurrent.CountDownLatch;

/**
 * Created by mingdongli
 * 2019/9/8 上午1:16.
 */
public interface NettyClientWithEndpoint extends NettyClient {

    Endpoint endpoint();

    CountDownLatch getCountDownLatch();

}
