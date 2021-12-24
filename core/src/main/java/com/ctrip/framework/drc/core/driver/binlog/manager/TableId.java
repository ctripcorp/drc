package com.ctrip.framework.drc.core.driver.binlog.manager;

import java.util.Objects;

/**
 * Created by mingdongli
 * 2019/9/29 下午2:23.
 */
public class TableId {

    private String dbName;

    private String tableName;

    public TableId() {
    }

    public TableId(String dbName, String tableName) {
        this.dbName = dbName;
        this.tableName = tableName;
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableId tableId = (TableId) o;
        return Objects.equals(dbName, tableId.dbName) &&
                Objects.equals(tableName, tableId.tableName);
    }

    @Override
    public int hashCode() {

        return Objects.hash(dbName, tableName);
    }
}
