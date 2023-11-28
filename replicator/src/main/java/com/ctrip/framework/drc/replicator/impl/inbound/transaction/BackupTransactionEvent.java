package com.ctrip.framework.drc.replicator.impl.inbound.transaction;

import com.ctrip.framework.drc.core.driver.binlog.impl.TransactionEvent;

/**
 * @Author limingdong
 * @create 2020/8/27
 */
public class BackupTransactionEvent extends TransactionEvent {

    @Override
    public boolean passFilter() {
        return false;
    }

    @Override
    public void addFilterLogEvent() {
    }
}
