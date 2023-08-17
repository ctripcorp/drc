package com.ctrip.framework.drc.core.driver.command.netty.codec;

import io.netty.channel.Channel;

/**
 * Created by jixinwang on 2023/8/15
 */
public interface FileCheck {

    void setChannel(Channel channel);

    void setFileManager(FileManager fileManager);

    void startCheck();

    void stopCheck();

    boolean check();
}
