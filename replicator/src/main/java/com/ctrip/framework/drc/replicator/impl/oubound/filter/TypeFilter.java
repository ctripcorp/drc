package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;

/**
 * Applier and row filter configured
 *
 * @Author limingdong
 * @create 2022/4/22
 */
public class TypeFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    private ConsumeType consumeType;

    public TypeFilter(ConsumeType consumeType) {
        this.consumeType = consumeType;
    }

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        switch (consumeType) {
            case Applier:
            case Messenger:
                filterApplier(value);
                break;
            default:
        }

        return doNext(value, value.isSkipEvent());
    }

    private void filterApplier(OutboundLogEventContext value) {
        if (LogEventUtils.isApplierIgnored(value.getEventType())) {
            value.setSkipEvent(true);
        }
    }
}
