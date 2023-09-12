package com.ctrip.framework.drc.console.service.v2.external.dba.response;

import java.util.List;

public class DbClusterInfoDto {

    private String dbName;
    private List<ClusterInfoDto> clusterList;

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public List<ClusterInfoDto> getClusterList() {
        return clusterList;
    }

    public void setClusterList(List<ClusterInfoDto> clusterList) {
        this.clusterList = clusterList;
    }
}
