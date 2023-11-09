package com.ctrip.framework.drc.console.vo.log;

import java.sql.Timestamp;

/**
 * Created by dengquanliang
 * 2023/10/31 11:09
 */
public class ConflictApprovalView {
    private Long approvalId;
    private Long batchId;
    private String dbName;
    private String tableName;
    private Integer approvalResult;
    private String remark;
    private String applicant;
    private String createTime;
    private String approvalDetailUrl;

    public String getApprovalDetailUrl() {
        return approvalDetailUrl;
    }

    public void setApprovalDetailUrl(String approvalDetailUrl) {
        this.approvalDetailUrl = approvalDetailUrl;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Long getApprovalId() {
        return approvalId;
    }

    public void setApprovalId(Long approvalId) {
        this.approvalId = approvalId;
    }

    public Long getBatchId() {
        return batchId;
    }

    public void setBatchId(Long batchId) {
        this.batchId = batchId;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
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

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }
}
