package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.fetcher.event.transaction.TransactionContext;

/**
 * @Author Slight
 * Oct 13, 2019
 */
public interface TransactionContextManager {

    TransactionContext openTransactionContext() throws Exception;
    void closeTransactionContext(TransactionContext transactionContext);
}
