package com.ctrip.framework.drc.core.driver;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import io.netty.buffer.ByteBuf;

import java.util.Collection;

/**
 * @author wenchao.meng
 * <p>
 * Sep 01, 2019
 */
public interface IoCache {

    void write(byte[] data);

    void write(Collection<ByteBuf> byteBuf);

    void write(Collection<ByteBuf> byteBuf, boolean isDdl);

    void write(LogEvent logEvent);

}
