package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.common.filter.FilterChainFactory;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.extract.ExtractFilter;

/**
 * preFilter
 * ConsumeTypeFilter -> TableFilter -> RowsFilter
 * <p>
 * postFilter
 * SendFilter
 *
 * @Author limingdong
 * @create 2022/4/22
 */
public class OutboundFilterChainFactory implements FilterChainFactory<OutboundFilterChainContext, OutboundLogEventContext> {

    @Override
    public Filter<OutboundLogEventContext> createFilterChain(OutboundFilterChainContext context) {
        MonitorFilter monitorFilter = new MonitorFilter(context);

        SendFilter sendFilter = new SendFilter(context);
        monitorFilter.setSuccessor(sendFilter);

        ReadFilter readFilter = new ReadFilter(context.getRegisterKey());
        sendFilter.setSuccessor(readFilter);

        IndexFilter indexFilter = new IndexFilter(context.getExcludedSet());
        readFilter.setSuccessor(indexFilter);

        SkipFilter skipFilter = new SkipFilter(context);
        indexFilter.setSuccessor(skipFilter);

        TypeFilter consumeTypeFilter = new TypeFilter(context.getConsumeType());
        skipFilter.setSuccessor(consumeTypeFilter);

        if (ConsumeType.Replicator != context.getConsumeType()) {
            TableNameFilter tableNameFilter = new TableNameFilter(context.getAviatorFilter());
            consumeTypeFilter.setSuccessor(tableNameFilter);

            if (context.shouldExtract()) {
                ExtractFilter extractFilter = new ExtractFilter(context);
                tableNameFilter.setSuccessor(extractFilter);
            }
        }

        return monitorFilter;
    }
}
