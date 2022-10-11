package com.ctrip.framework.drc.core.driver;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TransactionContext;
import io.netty.buffer.ByteBuf;

import java.util.Collection;

/**
 * @author wenchao.meng
 * <p>
 * Sep 01, 2019
 */
public interface IoCache {

    void write(Collection<ByteBuf> byteBuf);

    default void write(Collection<ByteBuf> byteBuf, TransactionContext transactionContext) {}

    default void write(LogEvent logEvent) {}

}
