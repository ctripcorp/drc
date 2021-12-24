package com.ctrip.framework.drc.console.monitor.comparator;

import com.ctrip.framework.drc.core.meta.comparator.AbstractMetaComparator;

/**
 * Created by jixinwang on 2021/8/3
 */
public class ReplicatorWrapperComparator extends AbstractMetaComparator<String, ReplicatorWrapperChange> {

    private String dbClusterId;

    public ReplicatorWrapperComparator(String dbClusterId) {
        this.dbClusterId = dbClusterId;
    }

    public String getDbClusterId() {
        return dbClusterId;
    }

    @Override
    public void compare() {
    }

    @Override
    public String idDesc() {
        return null;
    }
}
