package com.ctrip.framework.drc.console.dto;

/**
 * Created by jixinwang on 2020/11/16
 */
public class DalClusterShard {

    private Integer shardIndex;

    private String dbName;

    private String zoneId;

    public DalClusterShard(Integer shardIndex, String dbName, String zoneId) {
        this.shardIndex = shardIndex;
        this.dbName = dbName;
        this.zoneId = zoneId;
    }

    public Integer getShardIndex() {
        return shardIndex;
    }

    public void setShardIndex(Integer shardIndex) {
        this.shardIndex = shardIndex;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getZoneId() {
        return zoneId;
    }

    public void setZoneId(String zoneId) {
        this.zoneId = zoneId;
    }
}
