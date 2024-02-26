package com.ctrip.framework.drc.console.dto.v3;

public class DbReplicationDto {
    private Long dbReplicationId;
    private LogicTableConfig logicTableConfig;


    public Long getDbReplicationId() {
        return dbReplicationId;
    }

    public void setDbReplicationId(Long dbReplicationId) {
        this.dbReplicationId = dbReplicationId;
    }

    public LogicTableConfig getLogicTableConfig() {
        return logicTableConfig;
    }

    public void setLogicTableConfig(LogicTableConfig logicTableConfig) {
        this.logicTableConfig = logicTableConfig;
    }

    public DbReplicationDto(Long dbReplicationId, LogicTableConfig config) {
        this.dbReplicationId = dbReplicationId;
        this.logicTableConfig = config;
    }
}
