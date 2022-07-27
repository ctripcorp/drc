package com.ctrip.framework.drc.fetcher.event.transaction;

import com.ctrip.framework.drc.fetcher.resource.context.BaseTransactionContext;

/**
 * @Author Slight
 * Sep 19, 2019
 */
public interface TerminateEvent<T extends BaseTransactionContext> extends TransactionEvent<T> {

    default ApplyResult terminate(T context) {
        return context.complete();
    }
}
