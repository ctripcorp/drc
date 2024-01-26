package com.ctrip.framework.drc.console.dao.log.entity;

import java.util.Objects;

/**
 * Created by dengquanliang
 * 2023/12/26 15:26
 */
public class ConflictRowsLogCount implements Comparable<ConflictRowsLogCount> {
    private String dbName;
    private String tableName;
    private Long rowLogId;
    private Long trxLogId;
    private int count;

    public ConflictRowsLogCount(String dbName, String tableName, Long rowLogId, Long trxLogId, int count) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.rowLogId = rowLogId;
        this.trxLogId = trxLogId;
        this.count = count;
    }

    public ConflictRowsLogCount() {
    }

    public Long getRowLogId() {
        return rowLogId;
    }

    public void setRowLogId(Long rowLogId) {
        this.rowLogId = rowLogId;
    }

    public Long getTrxLogId() {
        return trxLogId;
    }

    public void setTrxLogId(Long trxLogId) {
        this.trxLogId = trxLogId;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConflictRowsLogCount that = (ConflictRowsLogCount) o;
        return Objects.equals(dbName, that.dbName) && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbName, tableName);
    }

    @Override
    public String toString() {
        return "ConflictRowsLogCount{" +
                "dbName='" + dbName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", rowLogId=" + rowLogId +
                ", trxLogId=" + trxLogId +
                ", count=" + count +
                '}';
    }
}
