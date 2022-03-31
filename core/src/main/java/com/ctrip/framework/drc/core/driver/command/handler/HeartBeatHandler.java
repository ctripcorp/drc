package com.ctrip.framework.drc.core.driver.command.handler;

import io.netty.channel.Channel;

/**
 * @Author limingdong
 * @create 2022/3/29
 */
public interface HeartBeatHandler {

    void sendHeartBeat(Channel channel);
}
