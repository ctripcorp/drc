package com.ctrip.framework.drc.console.dto.log;

/**
 * Created by dengquanliang
 * 2023/10/12 19:19
 */
public class ConflictRowsLogDto {
    private String db;
    private String table;
    private String rawSql;
    private String rawSqlRes;
    private String dstRowRecord;
    private String handleSql;
    private String handleSqlRes;
    private Integer rowsRes;

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getRawSql() {
        return rawSql;
    }

    public void setRawSql(String rawSql) {
        this.rawSql = rawSql;
    }

    public String getRawSqlRes() {
        return rawSqlRes;
    }

    public void setRawSqlRes(String rawSqlRes) {
        this.rawSqlRes = rawSqlRes;
    }

    public String getDstRowRecord() {
        return dstRowRecord;
    }

    public void setDstRowRecord(String dstRowRecord) {
        this.dstRowRecord = dstRowRecord;
    }

    public String getHandleSql() {
        return handleSql;
    }

    public void setHandleSql(String handleSql) {
        this.handleSql = handleSql;
    }

    public String getHandleSqlRes() {
        return handleSqlRes;
    }

    public void setHandleSqlRes(String handleSqlRes) {
        this.handleSqlRes = handleSqlRes;
    }

    public Integer getRowsRes() {
        return rowsRes;
    }

    public void setRowsRes(Integer rowsRes) {
        this.rowsRes = rowsRes;
    }
}
