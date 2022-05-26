package com.ctrip.framework.drc.core.server.common.filter.service;

import com.ctrip.framework.drc.core.server.common.filter.row.UidContext;

/**
 * @Author limingdong
 * @create 2022/5/10
 */
public class LocalUidService implements UidService {

    @Override
    public boolean filterUid(UidContext uidContext) throws Exception {
        return true;
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
