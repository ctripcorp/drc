package com.ctrip.framework.drc.core.server.common.filter.service;

import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterResult;
import com.ctrip.framework.drc.core.server.common.filter.row.soa.CustomSoaRowFilterContext;
import com.ctrip.xpipe.api.lifecycle.Ordered;

public interface CustomSoaService extends Ordered {
    
    RowsFilterResult.Status filter(int ServiceCode,String serviceName, CustomSoaRowFilterContext soaRowFilterContext) throws Exception;

}
