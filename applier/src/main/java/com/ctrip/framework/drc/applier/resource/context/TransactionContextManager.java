package com.ctrip.framework.drc.applier.resource.context;

/**
 * @Author Slight
 * Oct 13, 2019
 */
public interface TransactionContextManager {

    TransactionContext openTransactionContext() throws Exception;
    void closeTransactionContext(TransactionContext transactionContext);
}
