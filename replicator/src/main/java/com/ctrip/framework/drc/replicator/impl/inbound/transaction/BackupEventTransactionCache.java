package com.ctrip.framework.drc.replicator.impl.inbound.transaction;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.impl.ITransactionEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TransactionEvent;
import com.ctrip.framework.drc.core.server.common.filter.Filter;

/**
 * @Author limingdong
 * @create 2020/8/27
 */
public class BackupEventTransactionCache extends EventTransactionCache {

    public BackupEventTransactionCache(IoCache ioCache, Filter<ITransactionEvent> filterChain) {
        super(ioCache, filterChain);
    }

    protected TransactionEvent getTransactionEvent() {
        return new BackupTransactionEvent();
    }
}
