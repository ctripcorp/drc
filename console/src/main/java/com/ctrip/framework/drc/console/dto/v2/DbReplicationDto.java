package com.ctrip.framework.drc.console.dto.v2;

/**
 * Created by dengquanliang
 * 2023/5/26 16:12
 */
public class DbReplicationDto {
    private Long dbReplicationId;
    private Long srcMhaDbMappingId;
    private String srcLogicTableName;

    public Long getDbReplicationId() {
        return dbReplicationId;
    }

    public void setDbReplicationId(Long dbReplicationId) {
        this.dbReplicationId = dbReplicationId;
    }

    public Long getSrcMhaDbMappingId() {
        return srcMhaDbMappingId;
    }

    public void setSrcMhaDbMappingId(Long srcMhaDbMappingId) {
        this.srcMhaDbMappingId = srcMhaDbMappingId;
    }

    public String getSrcLogicTableName() {
        return srcLogicTableName;
    }

    public void setSrcLogicTableName(String srcLogicTableName) {
        this.srcLogicTableName = srcLogicTableName;
    }

}
