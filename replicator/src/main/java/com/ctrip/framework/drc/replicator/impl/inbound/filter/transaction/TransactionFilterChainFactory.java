package com.ctrip.framework.drc.replicator.impl.inbound.filter.transaction;

import com.ctrip.framework.drc.core.driver.binlog.impl.ITransactionEvent;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.common.filter.FilterChainFactory;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.InboundFilterChainContext;

/**
 * Created by jixinwang on 2021/10/9
 */
public class TransactionFilterChainFactory implements FilterChainFactory<InboundFilterChainContext, ITransactionEvent> {

    public Filter<ITransactionEvent> createFilterChain(InboundFilterChainContext context) {
        TransactionOffsetFilter transactionOffsetFilter = new TransactionOffsetFilter();

        DdlIndexFilter ddlIndexFilter = new DdlIndexFilter();
        ddlIndexFilter.setSuccessor(transactionOffsetFilter);

        if (ApplyMode.transaction_table.getType() == context.getApplyMode()) {
            TypeConvertFilter typeConvertFilter = new TypeConvertFilter();
            typeConvertFilter.setSuccessor(ddlIndexFilter);
            return typeConvertFilter;
        }
        return ddlIndexFilter;
    }
}
