package com.ctrip.framework.drc.core.server.common.filter.service;

import java.util.Set;

/**
 * @Author limingdong
 * @create 2022/5/10
 */
public class LocalUidService implements UidService {

    @Override
    public boolean filterUid(String uid, Set<String> locations) throws Exception {
        return true;
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
