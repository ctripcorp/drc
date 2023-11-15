package com.ctrip.framework.drc.console.vo.log;

/**
 * Created by dengquanliang
 * 2023/10/31 19:58
 */
public class ConflictAutoHandleView {
    private Long rowLogId;
    private String dbName;
    private String tableName;
    private String autoHandleSql;

    public Long getRowLogId() {
        return rowLogId;
    }

    public void setRowLogId(Long rowLogId) {
        this.rowLogId = rowLogId;
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

    public String getAutoHandleSql() {
        return autoHandleSql;
    }

    public void setAutoHandleSql(String autoHandleSql) {
        this.autoHandleSql = autoHandleSql;
    }

    @Override
    public String toString() {
        return "ConflictAutoHandleView{" +
                "rowLogId=" + rowLogId +
                ", dbName='" + dbName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", autoHandleSql='" + autoHandleSql + '\'' +
                '}';
    }
}
