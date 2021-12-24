package com.ctrip.framework.drc.validation.event;

import com.ctrip.framework.drc.fetcher.event.transaction.AbstractTransaction;
import com.ctrip.framework.drc.fetcher.event.transaction.Transaction;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;
import com.ctrip.framework.drc.validation.event.transaction.BeginEvent;
import com.ctrip.framework.drc.validation.resource.context.ValidationTransactionContext;

/**
 * @author: Haibo Shen
 * @Date: 2021/3/17
 */
public class ValidationTransaction extends AbstractTransaction<ValidationTransactionContext> implements Transaction, TransactionData<ValidationTransactionContext> {

    private final BeginEvent beginEvent;

    public ValidationTransaction(BeginEvent beginEvent) {
        super(beginEvent);
        this.beginEvent = beginEvent;
    }

    @Override
    public String identifier() {
        return beginEvent.identifier();
    }
}
