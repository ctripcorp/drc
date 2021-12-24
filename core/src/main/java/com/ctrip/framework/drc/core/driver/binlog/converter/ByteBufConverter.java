package com.ctrip.framework.drc.core.driver.binlog.converter;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import io.netty.buffer.ByteBuf;

import java.util.List;

@FunctionalInterface
public interface ByteBufConverter {

    List<LogEvent> convert(final ByteBuf byteBuf);
}
