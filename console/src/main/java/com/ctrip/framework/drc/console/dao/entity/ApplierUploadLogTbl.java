package com.ctrip.framework.drc.console.dao.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;
import java.sql.Types;
import java.sql.Timestamp;

import com.ctrip.platform.dal.dao.DalPojo;

/**
 * @author wjx王继欣
 * @date 2020-01-20
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "applier_upload_log_tbl")
public class ApplierUploadLogTbl implements DalPojo {

    /**
     * 自增id
     */
    @Id
    @Column(name = "log_id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long logId;

    /**
     * 源数据中心
     */
    @Column(name = "src_dc_name")
    @Type(value = Types.VARCHAR)
    private String srcDcName;

    /**
     * 目的数据中心
     */
    @Column(name = "dest_dc_name")
    @Type(value = Types.VARCHAR)
    private String destDcName;

    /**
     * 集群名称
     */
    @Column(name = "cluster_name")
    @Type(value = Types.VARCHAR)
    private String clusterName;

    /**
     * 用户id
     */
    @Column(name = "uid")
    @Type(value = Types.VARCHAR)
    private String uid;

    /**
     * 日志类型
     */
    @Column(name = "log_type")
    @Type(value = Types.VARCHAR)
    private String logType;

    /**
     * 冲突或样本sql
     */
    @Column(name = "sql_statement")
    @Type(value = Types.LONGVARCHAR)
    private String sqlStatement;

    /**
     * sql执行时长
     */
    @Column(name = "sql_time")
    @Type(value = Types.BIGINT)
    private Long sqlTime;

    /**
     * 更新时间
     */
    @Column(name = "datachange_lasttime", insertable = false, updatable = false)
    @Type(value = Types.TIMESTAMP)
    private Timestamp datachangeLasttime;

    /**
     * 用户最后访问应用id
     */
    @Column(name = "last_access_app_id")
    @Type(value = Types.BIGINT)
    private Long lastAccessAppId;

    /**
     * 处理冲突sql
     */
    @Column(name = "sql_handle_conflict")
    @Type(value = Types.LONGVARCHAR)
    private String sqlHandleConflict;

    public Long getLogId() {
        return logId;
    }

    public void setLogId(Long logId) {
        this.logId = logId;
    }

    public String getSrcDcName() {
        return srcDcName;
    }

    public void setSrcDcName(String srcDcName) {
        this.srcDcName = srcDcName;
    }

    public String getDestDcName() {
        return destDcName;
    }

    public void setDestDcName(String destDcName) {
        this.destDcName = destDcName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getLogType() {
        return logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }

    public String getSqlStatement() {
        return sqlStatement;
    }

    public void setSqlStatement(String sqlStatement) {
        this.sqlStatement = sqlStatement;
    }

    public Long getSqlTime() {
        return sqlTime;
    }

    public void setSqlTime(Long sqlTime) {
        this.sqlTime = sqlTime;
    }

    public Timestamp getDatachangeLasttime() {
        return datachangeLasttime;
    }

    public void setDatachangeLasttime(Timestamp datachangeLasttime) {
        this.datachangeLasttime = datachangeLasttime;
    }

    public Long getLastAccessAppId() {
        return lastAccessAppId;
    }

    public void setLastAccessAppId(Long lastAccessAppId) {
        this.lastAccessAppId = lastAccessAppId;
    }

    public String getSqlHandleConflict() {
        return sqlHandleConflict;
    }

    public void setSqlHandleConflict(String sqlHandleConflict) {
        this.sqlHandleConflict = sqlHandleConflict;
    }

}
