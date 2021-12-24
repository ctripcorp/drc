package com.ctrip.framework.drc.replicator.impl.inbound.filter.transaction;

import com.ctrip.framework.drc.core.driver.binlog.impl.ITransactionEvent;

/**
 * @Author limingdong
 * @create 2021/11/24
 */
public class DdlIndexFilter extends AbstractTransactionFilter {

    @Override
    public boolean doFilter(ITransactionEvent transactionEvent) {
        canSkipParseTransaction(transactionEvent);
        return doNext(transactionEvent, !transactionEvent.passFilter());  // whether skip filters afterward
    }
}
