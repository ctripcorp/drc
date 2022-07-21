package com.ctrip.framework.drc.fetcher.event;

import com.ctrip.framework.drc.fetcher.event.transaction.TransactionContext;
import com.ctrip.framework.drc.fetcher.event.meta.MetaEvent;
import com.ctrip.framework.drc.fetcher.resource.condition.DirectMemoryAware;
import com.ctrip.framework.drc.fetcher.resource.context.EventGroupContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author Slight
 * Sep 30, 2019
 */
public class ApplierXidEvent extends MonitoredXidEvent<TransactionContext> implements MetaEvent.Write<EventGroupContext>, DirectMemoryAware {

    protected static final Logger loggerED = LoggerFactory.getLogger("EVT DELAY");

    public ApplierXidEvent() {
        super();
    }

    @Override
    public ApplyResult apply(TransactionContext context) {
        super.apply(context);
        return terminate0(context);
    }

    protected ApplyResult terminate0(TransactionContext context) {
        return context.complete();
    }

}
