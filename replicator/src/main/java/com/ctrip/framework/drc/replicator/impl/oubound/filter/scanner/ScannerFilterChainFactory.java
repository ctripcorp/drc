package com.ctrip.framework.drc.replicator.impl.oubound.filter.scanner;

import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.common.filter.FilterChainFactory;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.*;


/**
 * @author yongnian
 * <p>
 * preFilter
 * ReadFilter -> IndexFilter -> TypeFilter -> ScannerSchemaFilter -> ScannerSkipFilter -> ScannerTableNameFilter
 * <p>
 * postFilter
 * ScannerPositionFilter
 */
public class ScannerFilterChainFactory implements FilterChainFactory<ScannerFilterChainContext, OutboundLogEventContext> {

    @Override
    public Filter<OutboundLogEventContext> createFilterChain(ScannerFilterChainContext context) {
        ScannerPositionFilter sendFilter = new ScannerPositionFilter();

        ReadFilter readFilter = new ReadFilter(context.getRegisterKey());
        sendFilter.setSuccessor(readFilter);

        IndexFilter indexFilter = new IndexFilter(context);
        readFilter.setSuccessor(indexFilter);

        TypeFilter consumeTypeFilter = new TypeFilter(context.getConsumeType());
        indexFilter.setSuccessor(consumeTypeFilter);

        if (ConsumeType.Replicator != context.getConsumeType()) {
            ScannerSchemaFilter schemaFilter = new ScannerSchemaFilter(context);
            consumeTypeFilter.setSuccessor(schemaFilter);

            ScannerSkipFilter skipFilter = new ScannerSkipFilter(context);
            schemaFilter.setSuccessor(skipFilter);

            ScannerTableNameFilter tableNameFilter = new ScannerTableNameFilter(context);
            skipFilter.setSuccessor(tableNameFilter);
        } else {
            ScannerSkipFilter skipFilter = new ScannerSkipFilter(context);
            consumeTypeFilter.setSuccessor(skipFilter);
        }

        return sendFilter;
    }
}
