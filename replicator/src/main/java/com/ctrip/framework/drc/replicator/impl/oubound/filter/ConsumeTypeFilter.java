package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class ConsumeTypeFilter extends AbstractLogEventFilter<LogEventOutboundContext> {

    @Override
    public boolean doFilter(LogEventOutboundContext value) {
        return false;
    }
}
