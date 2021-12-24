package com.ctrip.framework.drc.applier.event;

import com.ctrip.framework.drc.applier.event.transaction.BeginEvent;
import com.ctrip.framework.drc.applier.event.transaction.LWMAware;
import com.ctrip.framework.drc.applier.event.transaction.LWMSource;
import com.ctrip.framework.drc.applier.event.transaction.Transaction;
import com.ctrip.framework.drc.applier.resource.condition.LWM;
import com.ctrip.framework.drc.applier.resource.condition.LWMPassHandler;
import com.ctrip.framework.drc.applier.resource.context.TransactionContext;
import com.ctrip.framework.drc.fetcher.event.transaction.AbstractTransaction;

/**
 * @Author Slight
 * Jul 07, 2020
 */
public class ApplierTransaction extends AbstractTransaction<TransactionContext> implements Transaction<TransactionContext>, LWMAware, LWMSource {

    private final BeginEvent beginEvent;

    public ApplierTransaction(BeginEvent beginEvent) {
        super(beginEvent);
        this.beginEvent = beginEvent;
    }

    @Override
    public void begin(LWM lwm, LWMPassHandler handler) throws InterruptedException {
        beginEvent.begin(lwm, handler);
    }

    @Override
    public void commit(LWM lwm) throws InterruptedException {
        beginEvent.commit(lwm);
    }

    @Override
    public String identifier() {
        return beginEvent.identifier();
    }
}
