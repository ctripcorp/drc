package com.ctrip.framework.drc.validation.event;

import com.ctrip.framework.drc.fetcher.event.MonitoredGtidLogEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.BaseBeginEvent;
import com.ctrip.framework.drc.fetcher.resource.context.EventGroupContext;
import com.ctrip.framework.drc.validation.resource.context.ValidationTransactionContext;

/**
 * @Author limingdong
 * @create 2021/3/16
 */
public class ValidationGtidEvent extends MonitoredGtidLogEvent<ValidationTransactionContext> implements BaseBeginEvent<ValidationTransactionContext> {

    @Override
    public void involve(EventGroupContext context) {
        context.begin(getGtid());
    }
}
