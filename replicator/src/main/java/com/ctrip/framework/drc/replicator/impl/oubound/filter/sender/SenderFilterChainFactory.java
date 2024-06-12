package com.ctrip.framework.drc.replicator.impl.oubound.filter.sender;

import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.common.filter.FilterChainFactory;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.*;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.extract.ExtractFilter;

/**
 * @author yongnian
 * <p>
 * preFilter
 * SenderSkipFilter -> SenderSchemaFilter -> SenderTableNameFilter -> ExtractFilter
 * <p>
 * postFilter
 * MonitorFilter -> SendFilter
 */
public class SenderFilterChainFactory implements FilterChainFactory<SenderFilterChainContext, OutboundLogEventContext> {

    @Override
    public Filter<OutboundLogEventContext> createFilterChain(SenderFilterChainContext context) {
        MonitorFilter monitorFilter = new MonitorFilter(context);

        SendFilter sendFilter = new SendFilter(context);
        monitorFilter.setSuccessor(sendFilter);

        SenderSkipFilter skipFilter = new SenderSkipFilter(context);
        sendFilter.setSuccessor(skipFilter);

        if (ConsumeType.Replicator != context.getConsumeType()) {
            SenderSchemaFilter schemaFilter = new SenderSchemaFilter(context);
            skipFilter.setSuccessor(schemaFilter);

            SenderTableNameFilter tableNameFilter = new SenderTableNameFilter(context);
            schemaFilter.setSuccessor(tableNameFilter);

            if (context.shouldExtract()) {
                ExtractFilter extractFilter = new ExtractFilter(context);
                tableNameFilter.setSuccessor(extractFilter);
            }
        }

        return monitorFilter;
    }
}
