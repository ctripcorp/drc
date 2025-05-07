package com.ctrip.framework.drc.console.dto.v3;

import java.sql.Timestamp;

public class DbReplicationDto {
    private Long dbReplicationId;
    private LogicTableConfig logicTableConfig;
    private Timestamp datachangeLasttime;


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

    public DbReplicationDto(Long dbReplicationId, LogicTableConfig config, Timestamp datachangeLasttime) {
        this.dbReplicationId = dbReplicationId;
        this.logicTableConfig = config;
        this.datachangeLasttime = datachangeLasttime;
    }

    public Timestamp getDatachangeLasttime() {
        return datachangeLasttime;
    }

    public void setDatachangeLasttime(Timestamp datachangeLasttime) {
        this.datachangeLasttime = datachangeLasttime;
    }
}
