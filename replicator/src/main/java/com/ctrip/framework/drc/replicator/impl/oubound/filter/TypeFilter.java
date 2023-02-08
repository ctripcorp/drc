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

    private boolean shouldExtract;

    public TypeFilter(ConsumeType consumeType, boolean shouldExtract) {
        this.consumeType = consumeType;
        this.shouldExtract = shouldExtract;
    }

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        if (ConsumeType.Applier == consumeType && LogEventUtils.isApplierIgnored(value.getEventType())) {
            value.setFilteredEventSize(0);
            value.setSkipEvent(true);
            return doNext(value, true);
        }
        if (ConsumeType.Applier != consumeType || !shouldExtract) {
            value.setNoRewrite(true);
        }
        return doNext(value, value.isNoRewrite());
    }
}
