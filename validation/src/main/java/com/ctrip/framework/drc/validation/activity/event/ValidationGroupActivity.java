package com.ctrip.framework.drc.validation.activity.event;

import com.ctrip.framework.drc.fetcher.activity.event.GroupActivity;
import com.ctrip.framework.drc.fetcher.event.transaction.BeginEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.Transaction;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionEvent;
import com.ctrip.framework.drc.validation.event.ValidationTransaction;

/**
 * @author: Haibo Shen
 * @Date: 2021/3/17
 */
public class ValidationGroupActivity extends GroupActivity {
    @Override
    protected Transaction getTransaction(BeginEvent b) {
        com.ctrip.framework.drc.validation.event.transaction.BeginEvent beginEvent = (com.ctrip.framework.drc.validation.event.transaction.BeginEvent) b;
        return new ValidationTransaction(beginEvent);
    }

    @Override
    protected TransactionEvent getRollbackEvent() {
        return null;
    }
}
