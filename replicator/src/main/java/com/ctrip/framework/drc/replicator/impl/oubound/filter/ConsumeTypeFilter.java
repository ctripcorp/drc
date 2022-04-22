package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class ConsumeTypeFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        if (ConsumeType.Applier != value.getConsumeType()) {
            value.setLineFilter(false);
        }
        return doNext(value, value.isLineFilter());
    }
}
