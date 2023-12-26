package com.ctrip.framework.drc.console.dao.log.entity;

/**
 * Created by dengquanliang
 * 2023/12/26 15:26
 */
public class ConflictRowsLogCount {
    private String dbName;
    private String tableName;
    private int count;

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

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
