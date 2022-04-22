package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;

/**
 * just handle rows event for line filter
 * @Author limingdong
 * @create 2022/4/22
 */
public class EventTypeFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        if (!LogEventUtils.isRowsEvent(value.getEventType())) {
            value.setLineFilter(false);
        }
        return doNext(value, value.isLineFilter());
    }
}
