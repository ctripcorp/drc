package com.ctrip.framework.drc.core.driver.binlog.impl;

import io.netty.buffer.ByteBuf;

public class DrcUnknownEvent extends AbstractLogEvent {
    @Override
    public DrcUnknownEvent read(ByteBuf byteBuf) {
        super.read(byteBuf);
        return this;
    }
}
