package com.ctrip.framework.drc.replicator.impl.inbound.filter.transaction;

import com.ctrip.framework.drc.core.driver.binlog.impl.ITransactionEvent;
import com.ctrip.framework.drc.core.server.common.Filter;
import com.ctrip.framework.drc.replicator.impl.monitor.MonitorManager;

/**
 * Created by jixinwang on 2021/10/9
 */
public class DefaultTransactionFilterChainFactory {

    public static Filter<ITransactionEvent> createFilterChain(MonitorManager delayMonitor) {
        TransactionOffsetFilter transactionOffsetFilter = new TransactionOffsetFilter();

        TransactionTableFilter transactionTableFilter = new TransactionTableFilter(delayMonitor);
        transactionTableFilter.setSuccessor(transactionOffsetFilter);

        DdlIndexFilter ddlIndexFilter = new DdlIndexFilter();
        ddlIndexFilter.setSuccessor(transactionTableFilter);

        return ddlIndexFilter;
    }
}
