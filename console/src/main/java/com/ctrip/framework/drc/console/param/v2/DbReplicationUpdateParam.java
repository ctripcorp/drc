package com.ctrip.framework.drc.console.param.v2;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/9/6 14:42
 */
public class DbReplicationUpdateParam {
    private List<Long> dbReplicationIds;
    private String dbName;
    private String tableName;

    public List<Long> getDbReplicationIds() {
        return dbReplicationIds;
    }

    public void setDbReplicationIds(List<Long> dbReplicationIds) {
        this.dbReplicationIds = dbReplicationIds;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return "DbReplicationUpdateParam{" +
                "dbReplicationIds=" + dbReplicationIds +
                ", dbName='" + dbName + '\'' +
                ", tableName='" + tableName + '\'' +
                '}';
    }
}
