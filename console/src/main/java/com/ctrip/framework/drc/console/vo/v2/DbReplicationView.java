package com.ctrip.framework.drc.console.vo.v2;

/**
 * Created by dengquanliang
 * 2023/8/2 10:34
 */
public class DbReplicationView {
    private Long dbReplicationId;
    private String dbName;
    private String logicTableName;

    public Long getDbReplicationId() {
        return dbReplicationId;
    }

    public void setDbReplicationId(Long dbReplicationId) {
        this.dbReplicationId = dbReplicationId;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getLogicTableName() {
        return logicTableName;
    }

    public void setLogicTableName(String logicTableName) {
        this.logicTableName = logicTableName;
    }
}
