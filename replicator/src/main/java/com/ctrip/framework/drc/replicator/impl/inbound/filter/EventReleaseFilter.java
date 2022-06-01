package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.server.common.filter.AbstractPostLogEventFilter;

/**
 * post filter
 * @Author limingdong
 * @create 2020/4/24

 */
public class EventReleaseFilter extends AbstractPostLogEventFilter<InboundLogEventContext> {

    @Override
    public boolean doFilter(InboundLogEventContext value) {

        boolean filtered = doNext(value, value.isInExcludeGroup());  //post filter
        value.releaseEvent();
        return filtered;

    }

}
