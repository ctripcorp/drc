package com.ctrip.framework.drc.core.server.common.filter.service;

import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterResult.Status;
import com.ctrip.framework.drc.core.server.common.filter.row.soa.CustomSoaRowFilterContext;

/**
 * @ClassName BlankCustomSoaService
 * @Author haodongPan
 * @Date 2024/7/19 16:56
 * @Version: $
 */
public class BlankCustomSoaService implements CustomSoaService {

    @Override
    public int getOrder() {
        return 1;
    }

    @Override
    public Status filter(int ServiceCode, String serviceName, CustomSoaRowFilterContext soaRowFilterContext) throws Exception {
        return null;
    }
}
