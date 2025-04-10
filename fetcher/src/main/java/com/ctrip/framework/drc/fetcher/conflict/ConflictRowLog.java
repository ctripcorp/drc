package com.ctrip.framework.drc.fetcher.conflict;

import com.ctrip.framework.drc.core.monitor.enums.ConflictDetail;

import java.util.Objects;

/**
 * @ClassName ConflictRowLog
 * @Author haodongPan
 * @Date 2023/9/25 19:40
 * @Version: $
 */
public class ConflictRowLog implements Comparable<ConflictRowLog> {
    private String db;
    private String table;
    private String rawSql;
    private String rawRes;
    private String dstRecord;
    private String handleSql;
    private String handleSqlRes;
    private int rowRes; // 0-commit 1-rollback
    /**
     * @see ConflictDetail
     */
    private String conflictDetail;
    private long rowId;
    
    public void brief () {
        rawSql = null;
        rawRes = null;
        dstRecord = null;
        handleSql = null;
        handleSqlRes = null;
    }
    
    @Override
    public int compareTo(ConflictRowLog another) {
        // compare rowRes first, then rowId ,rollback > commit, rowId desc
        if (this.rowRes == another.rowRes) {
            return (int) (another.getRowId() - this.rowId);
        } else {
            return this.rowRes - another.rowRes;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConflictRowLog that = (ConflictRowLog) o;
        return rowRes == that.rowRes && rowId == that.rowId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowRes, rowId);
    }

    @Override
    public String toString() {
        return "ConflictRowLog{" +
                "db='" + db + '\'' +
                ", table='" + table + '\'' +
                ", rawSql='" + rawSql + '\'' +
                ", rawRes='" + rawRes + '\'' +
                ", dstRecord='" + dstRecord + '\'' +
                ", handleSql='" + handleSql + '\'' +
                ", handleSqlRes='" + handleSqlRes + '\'' +
                ", rowRes=" + rowRes +
                ", rowId=" + rowId +
                '}';
    }

    public String getConflictDetail() {
        return conflictDetail;
    }

    public void setConflictDetail(ConflictDetail conflictDetail) {
        this.conflictDetail = conflictDetail.name();
    }

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

    public int getRowRes() {
        return rowRes;
    }

    public void setRowRes(int rowRes) {
        this.rowRes = rowRes;
    }

    public long getRowId() {
        return rowId;
    }

    public void setRowId(long rowId) {
        this.rowId = rowId;
    }
}
