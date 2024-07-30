package com.ctrip.framework.drc.replicator.impl.oubound.filter.scanner;

import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.common.filter.FilterChainFactory;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.*;


/**
 * @author yongnian
 * <p>
 * preFilter
 * ReadFilter -> IndexFilter -> ScannerSkipFilter -> TypeFilter -> ScannerSchemaFilter -> ScannerTableNameFilter
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

        IndexFilter indexFilter = new IndexFilter(context.getExcludedSet());
        readFilter.setSuccessor(indexFilter);

        ScannerSkipFilter skipFilter = new ScannerSkipFilter(context);
        indexFilter.setSuccessor(skipFilter);

        TypeFilter consumeTypeFilter = new TypeFilter(context.getConsumeType());
        skipFilter.setSuccessor(consumeTypeFilter);

        if (ConsumeType.Replicator != context.getConsumeType()) {
            ScannerSchemaFilter schemaFilter = new ScannerSchemaFilter(context);
            consumeTypeFilter.setSuccessor(schemaFilter);

            ScannerTableNameFilter tableNameFilter = new ScannerTableNameFilter(context);
            schemaFilter.setSuccessor(tableNameFilter);
        }

        return sendFilter;
    }
}
