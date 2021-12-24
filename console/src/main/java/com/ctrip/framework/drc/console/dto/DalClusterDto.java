package com.ctrip.framework.drc.console.dto;

import java.util.List;

/**
 * Created by jixinwang on 2020/11/16
 */
public class DalClusterDto {
    private String clusterName;

    private String baseDbName;

    private List<DalClusterShard> shards;

    public DalClusterDto(String clusterName, String baseDbName, List<DalClusterShard> shards) {
        this.clusterName = clusterName;
        this.baseDbName = baseDbName;
        this.shards = shards;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getBaseDbName() {
        return baseDbName;
    }

    public void setBaseDbName(String baseDbName) {
        this.baseDbName = baseDbName;
    }

    public List<DalClusterShard> getShards() {
        return shards;
    }

    public void setShards(List<DalClusterShard> shards) {
        this.shards = shards;
    }
}
