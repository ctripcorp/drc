package com.ctrip.framework.drc.core.driver;

import com.ctrip.framework.drc.core.driver.binlog.impl.TransactionContext;
import io.netty.buffer.ByteBuf;

import java.util.Collection;

/**
 * @author wenchao.meng
 * <p>
 * Sep 01, 2019
 */
public interface IoCache {

    /**
     * write single log event
     * @param byteBuf
     */
    void write(Collection<ByteBuf> byteBuf);

    /**
     * write transaction
     * @param byteBuf
     * @param transactionContext
     */
    default void write(Collection<ByteBuf> byteBuf, TransactionContext transactionContext) {}
}
