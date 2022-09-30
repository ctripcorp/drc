package com.ctrip.framework.drc.replicator.impl.inbound.filter.transaction;

import com.ctrip.framework.drc.core.driver.binlog.impl.ITransactionEvent;
import com.ctrip.framework.drc.core.server.common.filter.Filter;

/**
 * Created by jixinwang on 2021/10/9
 */
public class DefaultTransactionFilterChainFactory {

    public static Filter<ITransactionEvent> createFilterChain() {
        TransactionOffsetFilter transactionOffsetFilter = new TransactionOffsetFilter();

        DdlIndexFilter ddlIndexFilter = new DdlIndexFilter();
        ddlIndexFilter.setSuccessor(transactionOffsetFilter);

        TypeConvertFilter typeConvertFilter = new TypeConvertFilter();
        typeConvertFilter.setSuccessor(ddlIndexFilter);

        return typeConvertFilter;
    }
}
