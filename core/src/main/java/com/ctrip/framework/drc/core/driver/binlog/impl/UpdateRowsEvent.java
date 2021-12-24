package com.ctrip.framework.drc.core.driver.binlog.impl;


import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 * <p>
 * Sep 01, 2019
 */
public class UpdateRowsEvent extends AbstractRowsEvent {

    @Override
    public UpdateRowsEvent read(ByteBuf byteBuf) {
        final LogEvent logEvent = super.read(byteBuf);
        if (null == logEvent) {
            return null;
        }

        return this;
    }

    @Override
    public void write(IoCache ioCache) {
        super.write(ioCache);
    }
}
