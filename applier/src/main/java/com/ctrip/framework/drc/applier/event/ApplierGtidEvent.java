package com.ctrip.framework.drc.applier.event;

import com.ctrip.framework.drc.applier.event.transaction.BeginEvent;
import com.ctrip.framework.drc.applier.resource.condition.LWM;
import com.ctrip.framework.drc.applier.resource.condition.LWMPassHandler;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionContext;
import com.ctrip.framework.drc.fetcher.event.MonitoredGtidLogEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;

/**
 * @Author Slight
 * Sep 29, 2019
 */
public class ApplierGtidEvent extends MonitoredGtidLogEvent<TransactionContext> implements BeginEvent<TransactionContext> {

    public ApplierGtidEvent() {
        super();
    }

    public ApplierGtidEvent(String gtid) {
        super(gtid);
    }

    @Override
    public TransactionData.ApplyResult apply(TransactionContext context) {
        beginContext(context);
        return super.apply(context);
    }

    protected void beginContext(TransactionContext context) {
        context.setGtid(getGtid());
        context.begin();
    }

    @Override
    public void begin(LWM lwm, LWMPassHandler handler) throws InterruptedException {
        lwm.acquire(getSequenceNumber());
        lwm.onCommit(getLastCommitted(), handler, identifier());
        loggerEGS.info("GTID: {}", identifier());
    }

    @Override
    public void commit(LWM lwm) throws InterruptedException {
        lwm.commit(getSequenceNumber());
    }
}
