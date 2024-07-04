package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.replicator.impl.oubound.binlog.BinlogSender;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.sender.SenderFilterChainContext;

/**
 * @author yongnian
 * gtid、query、tablemap1、tablemap2、rows1、rows2、xid
 */
public class SenderTableNameFilter extends TableNameFilter {
    private final BinlogSender binlogSender;

    public SenderTableNameFilter(SenderFilterChainContext context) {
        this.binlogSender = context.getBinlogSender();
    }

    @Override
    protected boolean shouldSkipTableMapEvent(String tableName) {
        return !binlogSender.concernTable(tableName);
    }
}
