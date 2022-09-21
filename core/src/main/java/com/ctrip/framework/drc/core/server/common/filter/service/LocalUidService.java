package com.ctrip.framework.drc.core.server.common.filter.service;

import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterResult;
import com.ctrip.framework.drc.core.server.common.filter.row.UserContext;

/**
 * @Author limingdong
 * @create 2022/5/10
 */
public class LocalUidService implements UidService {

    @Override
    public RowsFilterResult.Status filterUid(UserContext uidContext) throws Exception {
        return RowsFilterResult.Status.from(true);
    }

    @Override
    public RowsFilterResult.Status filterUdl(UserContext uidContext) throws Exception {
        return RowsFilterResult.Status.from(true);
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
