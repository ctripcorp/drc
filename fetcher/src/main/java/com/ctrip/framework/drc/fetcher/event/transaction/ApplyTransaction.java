package com.ctrip.framework.drc.fetcher.event.transaction;

import com.ctrip.framework.drc.fetcher.resource.context.BaseTransactionContext;

/**
 * @Author Slight
 * Jul 07, 2020
 */
public interface ApplyTransaction<T extends BaseTransactionContext> extends com.ctrip.framework.drc.fetcher.event.transaction.Transaction, LWMAware, LWMSource, TransactionData<T> {

}
