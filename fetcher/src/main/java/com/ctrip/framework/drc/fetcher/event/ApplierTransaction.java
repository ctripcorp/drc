package com.ctrip.framework.drc.fetcher.event;

import com.ctrip.framework.drc.fetcher.event.transaction.*;
import com.ctrip.framework.drc.fetcher.resource.condition.LWM;
import com.ctrip.framework.drc.fetcher.resource.condition.LWMPassHandler;

/**
 * @Author Slight
 * Jul 07, 2020
 */
public class ApplierTransaction extends AbstractTransaction<TransactionContext> implements ApplyTransaction<TransactionContext>, LWMAware, LWMSource {

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
