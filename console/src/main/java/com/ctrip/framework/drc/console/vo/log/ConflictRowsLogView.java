package com.ctrip.framework.drc.console.vo.log;

/**
 * Created by dengquanliang
 * 2023/9/26 15:19
 */
public class ConflictRowsLogView {
    private Long conflictRowsLogId;
    private String gtid;
    private String dbName;
    private String tableName;
    private String handleTime;
    private Integer rowResult;
    private String srcRegion;
    private String dstRegion;
    private String rawSql;
    private String rawSqlResult;
    private String handleSql;
    private String handleSqlResult;
    private String dstRowRecord;
    private Long conflictTrxLogId;


    public String getRawSqlResult() {
        return rawSqlResult;
    }

    public void setRawSqlResult(String rawSqlResult) {
        this.rawSqlResult = rawSqlResult;
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

    public String getHandleSql() {
        return handleSql;
    }

    public void setHandleSql(String handleSql) {
        this.handleSql = handleSql;
    }

    public Long getConflictTrxLogId() {
        return conflictTrxLogId;
    }

    public void setConflictTrxLogId(Long conflictTrxLogId) {
        this.conflictTrxLogId = conflictTrxLogId;
    }

    public String getRawSql() {
        return rawSql;
    }

    public void setRawSql(String rawSql) {
        this.rawSql = rawSql;
    }

    public String getGtid() {
        return gtid;
    }

    public void setGtid(String gtid) {
        this.gtid = gtid;
    }

    public Long getConflictRowsLogId() {
        return conflictRowsLogId;
    }

    public void setConflictRowsLogId(Long conflictRowsLogId) {
        this.conflictRowsLogId = conflictRowsLogId;
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

    public String getHandleTime() {
        return handleTime;
    }

    public void setHandleTime(String handleTime) {
        this.handleTime = handleTime;
    }

    public Integer getRowResult() {
        return rowResult;
    }

    public void setRowResult(Integer rowResult) {
        this.rowResult = rowResult;
    }

    public String getSrcRegion() {
        return srcRegion;
    }

    public void setSrcRegion(String srcRegion) {
        this.srcRegion = srcRegion;
    }

    public String getDstRegion() {
        return dstRegion;
    }

    public void setDstRegion(String dstRegion) {
        this.dstRegion = dstRegion;
    }
}
