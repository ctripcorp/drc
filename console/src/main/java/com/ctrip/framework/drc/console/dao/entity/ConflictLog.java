package com.ctrip.framework.drc.console.dao.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Sensitive;
import com.ctrip.platform.dal.dao.annotation.Type;
import java.sql.Types;
import java.sql.Timestamp;

import com.ctrip.platform.dal.dao.DalPojo;

/**
 * @author wjx王继欣
 * @date 2020-10-25
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "conflict_log")
public class ConflictLog implements DalPojo {

    /**
     * 主键
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long id;

    /**
     * 源mha名称
     */
    @Column(name = "src_mha_name")
    @Type(value = Types.VARCHAR)
    private String srcMhaName;

    /**
     * 目标mha名称
     */
    @Column(name = "dest_mha_name")
    @Type(value = Types.VARCHAR)
    private String destMhaName;

    /**
     * dbcluster集群名称
     */
    @Column(name = "cluster_name")
    @Type(value = Types.VARCHAR)
    private String clusterName;

    /**
     * 用户id
     */
    @Column(name = "user_id")
    @Type(value = Types.VARCHAR)
    private String userId;

    /**
     * 原始sql
     */
    @Column(name = "raw_sql_list")
    @Type(value = Types.LONGVARCHAR)
    private String rawSqlList;

    /**
     * 原始sql执行结果
     */
    @Column(name = "raw_sql_executed_result_list")
    @Type(value = Types.LONGVARCHAR)
    private String rawSqlExecutedResultList;

    /**
     * sql开始执行时间
     */
    @Column(name = "sql_execute_time")
    @Type(value = Types.BIGINT)
    private Long sqlExecuteTime;

    /**
     * 发生冲突时的记录内容
     */
    @Column(name = "dest_current_record_list")
    @Type(value = Types.LONGVARCHAR)
    private String destCurrentRecordList;

    /**
     * 冲突处理sql
     */
    @Column(name = "conflict_handle_sql_list")
    @Type(value = Types.LONGVARCHAR)
    private String conflictHandleSqlList;

    /**
     * 冲突处理sql执行结果
     */
    @Column(name = "conflict_handle_sql_executed_result_list")
    @Type(value = Types.LONGVARCHAR)
    private String conflictHandleSqlExecutedResultList;

    /**
     * 冲突事务最终处理结果
     */
    @Column(name = "last_result")
    @Type(value = Types.CHAR)
    private String lastResult;

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

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getSrcMhaName() {
        return srcMhaName;
    }

    public void setSrcMhaName(String srcMhaName) {
        this.srcMhaName = srcMhaName;
    }

    public String getDestMhaName() {
        return destMhaName;
    }

    public void setDestMhaName(String destMhaName) {
        this.destMhaName = destMhaName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getRawSqlList() {
        return rawSqlList;
    }

    public void setRawSqlList(String rawSqlList) {
        this.rawSqlList = rawSqlList;
    }

    public String getRawSqlExecutedResultList() {
        return rawSqlExecutedResultList;
    }

    public void setRawSqlExecutedResultList(String rawSqlExecutedResultList) {
        this.rawSqlExecutedResultList = rawSqlExecutedResultList;
    }

    public Long getSqlExecuteTime() {
        return sqlExecuteTime;
    }

    public void setSqlExecuteTime(Long sqlExecuteTime) {
        this.sqlExecuteTime = sqlExecuteTime;
    }

    public String getDestCurrentRecordList() {
        return destCurrentRecordList;
    }

    public void setDestCurrentRecordList(String destCurrentRecordList) {
        this.destCurrentRecordList = destCurrentRecordList;
    }

    public String getConflictHandleSqlList() {
        return conflictHandleSqlList;
    }

    public void setConflictHandleSqlList(String conflictHandleSqlList) {
        this.conflictHandleSqlList = conflictHandleSqlList;
    }

    public String getConflictHandleSqlExecutedResultList() {
        return conflictHandleSqlExecutedResultList;
    }

    public void setConflictHandleSqlExecutedResultList(String conflictHandleSqlExecutedResultList) {
        this.conflictHandleSqlExecutedResultList = conflictHandleSqlExecutedResultList;
    }

    public String getLastResult() {
        return lastResult;
    }

    public void setLastResult(String lastResult) {
        this.lastResult = lastResult;
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

}

