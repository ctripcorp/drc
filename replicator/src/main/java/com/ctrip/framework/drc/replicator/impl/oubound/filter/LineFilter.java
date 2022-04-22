package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.server.common.filter.AbstractPostLogEventFilter;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class LineFilter extends AbstractPostLogEventFilter<LogEventOutboundContext> {

    @Override
    public boolean doFilter(LogEventOutboundContext value) {
        return false;
    }
}
