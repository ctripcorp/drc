package com.ctrip.framework.drc.console.dao.log.entity;

import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * Created by dengquanliang
 * 2023/10/30 20:05
 */
@Entity
@Database(name = "bbzfxdrclogdb_w")
@Table(name = "conflict_approval_tbl")
public class ConflictApprovalTbl {
    /**
     * 主键
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long id;

    /**
     * 批次id
     */
    @Column(name = "batch_id")
    @Type(value = Types.BIGINT)
    private Long batchId;

    /**
     * 审批结果 0-审批中 1-审批通过 2-审批未通过
     */
    @Column(name = "approval_result")
    @Type(value = Types.TINYINT)
    private Integer approvalResult;

    /**
     * 备注,审批未通过原因
     */
    @Column(name = "remark")
    @Type(value = Types.VARCHAR)
    private String remark;

    /**
     * 申请人
     */
    @Column(name = "applicant")
    @Type(value = Types.VARCHAR)
    private String applicant;

    /**
     * db owner审批
     */
    @Column(name = "db_approver")
    @Type(value = Types.VARCHAR)
    private String dbApprover;

    /**
     * dba审批
     */
    @Column(name = "dba_approver")
    @Type(value = Types.VARCHAR)
    private String dbaApprover;

    /**
     * 当前审批人, 0-db owner; 1-dba
     */
    @Column(name = "current_approver_type")
    @Type(value = Types.VARCHAR)
    private String currentApproverType;

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

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getBatchId() {
        return batchId;
    }

    public void setBatchId(Long batchId) {
        this.batchId = batchId;
    }

    public Integer getApprovalResult() {
        return approvalResult;
    }

    public void setApprovalResult(Integer approvalResult) {
        this.approvalResult = approvalResult;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public String getApplicant() {
        return applicant;
    }

    public void setApplicant(String applicant) {
        this.applicant = applicant;
    }

    public String getDbApprover() {
        return dbApprover;
    }

    public void setDbApprover(String dbApprover) {
        this.dbApprover = dbApprover;
    }

    public String getDbaApprover() {
        return dbaApprover;
    }

    public void setDbaApprover(String dbaApprover) {
        this.dbaApprover = dbaApprover;
    }

    public String getCurrentApproverType() {
        return currentApproverType;
    }

    public void setCurrentApproverType(String currentApproverType) {
        this.currentApproverType = currentApproverType;
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
}
