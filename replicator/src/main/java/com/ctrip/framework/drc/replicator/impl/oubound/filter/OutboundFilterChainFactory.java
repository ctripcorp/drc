package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.common.filter.FilterChainFactory;

/**
 *
 *  preFilter
 *  ConsumeTypeFilter -> TableFilter -> RowsFilter
 *
 *  postFilter
 *  SendFilter
 *
 * @Author limingdong
 * @create 2022/4/22
 */
public class OutboundFilterChainFactory implements FilterChainFactory<OutboundFilterChainContext, OutboundLogEventContext> {

    @Override
    public Filter<OutboundLogEventContext> createFilterChain(OutboundFilterChainContext context) {
        SendFilter sendFilter = new SendFilter(context.getChannel());

        TypeFilter consumeTypeFilter = new TypeFilter(context.getConsumeType(), context.shouldFilterRows());
        sendFilter.setSuccessor(consumeTypeFilter);

        TableFilter tableFilter = new TableFilter();
        consumeTypeFilter.setSuccessor(tableFilter);

        ExtractFilter extractFilter = new ExtractFilter(context.getDataMediaConfig(), context.getOutboundMonitorReport());
        tableFilter.setSuccessor(extractFilter);

        RowsFilter rowsFilter = new RowsFilter(context.getDataMediaConfig(), context.getOutboundMonitorReport());
        extractFilter.setSuccessor(rowsFilter);

        ColumnsFilter columnsFilter = new ColumnsFilter(context.getDataMediaConfig(), context.getOutboundMonitorReport());
        rowsFilter.setSuccessor(columnsFilter);

        return sendFilter;
    }
}
