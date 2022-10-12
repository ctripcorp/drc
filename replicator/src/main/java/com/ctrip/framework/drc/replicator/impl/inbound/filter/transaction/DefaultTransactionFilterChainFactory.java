package com.ctrip.framework.drc.replicator.impl.inbound.filter.transaction;

import com.ctrip.framework.drc.core.driver.binlog.impl.ITransactionEvent;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;

/**
 * Created by jixinwang on 2021/10/9
 */
public class DefaultTransactionFilterChainFactory {

    public static Filter<ITransactionEvent> createFilterChain(int applyMode) {
        TransactionOffsetFilter transactionOffsetFilter = new TransactionOffsetFilter();

        DdlIndexFilter ddlIndexFilter = new DdlIndexFilter();
        ddlIndexFilter.setSuccessor(transactionOffsetFilter);

        if (ApplyMode.transaction_table.getType() == applyMode) {
            TypeConvertFilter typeConvertFilter = new TypeConvertFilter();
            typeConvertFilter.setSuccessor(ddlIndexFilter);
            return typeConvertFilter;
        }
        return ddlIndexFilter;
    }
}
