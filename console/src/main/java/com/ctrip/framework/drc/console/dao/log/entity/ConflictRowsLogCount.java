package com.ctrip.framework.drc.console.dao.log.entity;

/**
 * Created by dengquanliang
 * 2023/12/26 15:26
 */
public class ConflictRowsLogCount implements Comparable<ConflictRowsLogCount> {
    private String dbName;
    private String tableName;
    private int count;

    public ConflictRowsLogCount(String dbName, String tableName, int count) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.count = count;
    }

    public ConflictRowsLogCount() {
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

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public int compareTo(ConflictRowsLogCount o) {
        return o.getCount() - this.count;
    }
}
