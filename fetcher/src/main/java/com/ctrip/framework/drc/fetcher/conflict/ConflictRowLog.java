package com.ctrip.framework.drc.fetcher.conflict;

/**
 * @ClassName ConflictRowLog
 * @Author haodongPan
 * @Date 2023/9/25 19:40
 * @Version: $
 */
public class ConflictRowLog {
    private String db;
    private String table;
    private String rawSql;
    private String rawRes;
    private String dstRecord;
    private String handleSql;
    private String handleSqlRes;

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

    public String getRawRes() {
        return rawRes;
    }

    public void setRawRes(String rawRes) {
        this.rawRes = rawRes;
    }

    public String getDstRecord() {
        return dstRecord;
    }

    public void setDstRecord(String dstRecord) {
        this.dstRecord = dstRecord;
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
}
