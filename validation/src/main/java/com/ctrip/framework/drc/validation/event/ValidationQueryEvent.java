package com.ctrip.framework.drc.validation.event;

import com.ctrip.framework.drc.fetcher.event.MonitoredQueryEvent;
import com.ctrip.framework.drc.validation.resource.context.ValidationTransactionContext;

public class ValidationQueryEvent extends MonitoredQueryEvent<ValidationTransactionContext> {

    @Override
    public ApplyResult apply(ValidationTransactionContext context) {
        context.setExecuteTime(getLogEventHeader().getEventTimestamp());
        return super.apply(context);
    }
}
