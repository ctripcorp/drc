package com.ctrip.framework.drc.core.driver.binlog;

import com.ctrip.framework.drc.core.driver.Packet;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 * <p>
 * Sep 01, 2019
 * https://dev.mysql.com/doc/internals/en/binlog-event.html
 */
public interface LogEvent<V extends LogEvent> extends Packet<V>, Releasable {

    LogEventType getLogEventType();

    LogEventHeader getLogEventHeader();

    ByteBuf getPayloadBuf();

    boolean isEmpty();
}
