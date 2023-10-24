package com.ctrip.framework.drc.console.dao.log.entity;

import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * Created by dengquanliang
 * 2023/9/26 14:01
 */
@Entity
@Database(name = "bbzfxdrclogdb_w")
@Table(name = "conflict_rows_log_tbl")
public class ConflictRowsLogTbl {
    /**
     * 主键
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long id;

    /**
     * 冲突事务记录主键id
     */
    @Column(name = "conflict_trx_log_id")
    @Type(value = Types.BIGINT)
    private Long conflictTrxLogId;

    /**
     * 库名
     */
    @Column(name = "db_name")
    @Type(value = Types.VARCHAR)
    private String dbName;

    /**
     * 表名
     */
    @Column(name = "table_name")
    @Type(value = Types.VARCHAR)
    private String tableName;

    /**
     * 原始sql
     */
    @Column(name = "raw_sql")
    @Type(value = Types.VARCHAR)
    private String rawSql;

    /**
     * 原始sql执行结果
     */
    @Column(name = "raw_sql_result")
    @Type(value = Types.VARCHAR)
    private String rawSqlResult;

    /**
     * 目标关联行记录
     */
    @Column(name = "dst_row_record")
    @Type(value = Types.VARCHAR)
    private String dstRowRecord;

    /**
     * 冲突处理sql
     */
    @Column(name = "handle_sql")
    @Type(value = Types.VARCHAR)
    private String handleSql;

    /**
     * 冲突处理sql执行结果
     */
    @Column(name = "handle_sql_result")
    @Type(value = Types.VARCHAR)
    private String handleSqlResult;

    /**
     * 事务处理结果: 0-commit 1-rollback
     */
    @Column(name = "row_result")
    @Type(value = Types.TINYINT)
    private Integer rowResult;

    /**
     * 事务处理时间
     */
    @Column(name = "handle_time")
    @Type(value = Types.BIGINT)
    private Long handleTime;

    /**
     * 执行顺序ID
     */
    @Column(name = "row_id")
    @Type(value = Types.BIGINT)
    private Long rowId;

    /**
     * 是否删除, 0-否; 1-是
     */
    @Column(name = "deleted")
    @Type(value = Types.TINYINT)
    private Integer deleted;

    /**
     * 创建时间
     */
    @Column(name = "create_time")
    @Type(value = Types.TIMESTAMP)
    private Timestamp createTime;

    /**
     * 更新时间
     */
    @Column(name = "datachange_lasttime", insertable = false, updatable = false)
    @Type(value = Types.TIMESTAMP)
    private Timestamp datachangeLasttime;

    /**
     * 源region
     */
    @Column(name = "src_region")
    @Type(value = Types.VARCHAR)
    private String srcRegion;

    /**
     * 目标region
     */
    @Column(name = "dst_region")
    @Type(value = Types.VARCHAR)
    private String dstRegion;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getConflictTrxLogId() {
        return conflictTrxLogId;
    }

    public void setConflictTrxLogId(Long conflictTrxLogId) {
        this.conflictTrxLogId = conflictTrxLogId;
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

    public String getHandleSqlResult() {
        return handleSqlResult;
    }

    public void setHandleSqlResult(String handleSqlResult) {
        this.handleSqlResult = handleSqlResult;
    }

    public Integer getRowResult() {
        return rowResult;
    }

    public void setRowResult(Integer rowResult) {
        this.rowResult = rowResult;
    }

    public Integer getDeleted() {
        return deleted;
    }

    public void setDeleted(Integer deleted) {
        this.deleted = deleted;
    }

    public Timestamp getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Timestamp createTime) {
        this.createTime = createTime;
    }

    public Timestamp getDatachangeLasttime() {
        return datachangeLasttime;
    }

    public void setDatachangeLasttime(Timestamp datachangeLasttime) {
        this.datachangeLasttime = datachangeLasttime;
    }

    public Long getHandleTime() {
        return handleTime;
    }

    public void setHandleTime(Long handleTime) {
        this.handleTime = handleTime;
    }

    public Long getRowId() {
        return rowId;
    }

    public void setRowId(Long rowId) {
        this.rowId = rowId;
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
