package com.ctrip.framework.drc.console.vo.log;

/**
 * Created by dengquanliang
 * 2023/10/9 19:49
 */
public class ConflictRowsLogDetailView {
    private String rawSql;
    private String rawSqlResult;
    private String handleSql;
    private String handleSqlResult;
    private String dstRowRecord;
    private Integer rowResult;

    public String getRawSql() {
        return rawSql;
    }

    public void setRawSql(String rawSql) {
        this.rawSql = rawSql;
    }

    public String getRawSqlResult() {
        return rawSqlResult;
    }

    public void setRawSqlResult(String rawSqlResult) {
        this.rawSqlResult = rawSqlResult;
    }

    public String getHandleSql() {
        return handleSql;
    }

    public void setHandleSql(String handleSql) {
        this.handleSql = handleSql;
    }

    public String getHandleSqlResult() {
        return handleSqlResult;
    }

    public void setHandleSqlResult(String handleSqlResult) {
        this.handleSqlResult = handleSqlResult;
    }

    public String getDstRowRecord() {
        return dstRowRecord;
    }

    public void setDstRowRecord(String dstRowRecord) {
        this.dstRowRecord = dstRowRecord;
    }

    public Integer getRowResult() {
        return rowResult;
    }

    public void setRowResult(Integer rowResult) {
        this.rowResult = rowResult;
    }
}
