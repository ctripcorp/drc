package com.ctrip.framework.drc.console.service.v2.impl.migrate;

import com.ctrip.framework.drc.core.entity.DbCluster;

/**
 * @ClassName DbClusterCompareRes
 * @Author haodongPan
 * @Date 2023/7/12 11:08
 * @Version: $
 */
public class DbClusterCompareRes {

    private String oldDbCluster;
    private String newDbCluster;
    private String compareRes;


    public String getOldDbCluster() {
        return oldDbCluster;
    }

    public void setOldDbCluster(DbCluster oldDbCluster) {
        this.oldDbCluster = oldDbCluster.toString();
    }

    public String getNewDbCluster() {
        return newDbCluster;
    }

    public void setNewDbCluster(DbCluster newDbCluster) {
        this.newDbCluster = newDbCluster.toString();
    }

    public String getCompareRes() {
        return compareRes;
    }

    public void setCompareRes(String compareRes) {
        this.compareRes = compareRes;
    }
}