package com.ctrip.framework.drc.replicator.impl.oubound.filter.extract;

import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.common.filter.FilterChainFactory;

/**
 * Created by jixinwang on 2022/12/29
 */
public class ExtractFilterChainFactory implements FilterChainFactory<ExtractFilterChainContext, ExtractFilterContext> {

    @Override
    public Filter<ExtractFilterContext> createFilterChain(ExtractFilterChainContext context) {
        RowsFilter rowsFilter = new RowsFilter(context);

        if (context.shouldFilterColumns()) {
            ColumnsFilter columnsFilter = new ColumnsFilter(context);
            rowsFilter.setSuccessor(columnsFilter);
        }

        return rowsFilter;
    }
}
