package com.ctrip.framework.drc.replicator.impl.oubound.filter.scanner;

import com.ctrip.framework.drc.replicator.impl.oubound.binlog.BinlogSender;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.OutboundLogEventContext;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.TableNameFilter;

import java.util.List;

/**
 * @author yongnian
 * gtid、query、tablemap1、tablemap2、rows1、rows2、xid
 */
public class ScannerTableNameFilter extends TableNameFilter {
    private final List<BinlogSender> senders;

    public ScannerTableNameFilter(ScannerFilterChainContext context) {
        this.senders = context.getScanner().getSenders();
    }


    @Override
    protected void filterDrcTableMapEvent(OutboundLogEventContext value) {
        value.readTableMapEvent();
    }

    @Override
    protected void filterRowsEvent(OutboundLogEventContext value) {
        super.filterRowsEvent(value);
        if (!value.isSkipEvent()) {
            value.readRowsEvent();
        }
    }

    @Override
    protected boolean shouldSkipTableMapEvent(String tableName) {
        return senders.stream().noneMatch(sender -> sender.concernTable(tableName));
    }
}
