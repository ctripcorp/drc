package com.ctrip.framework.drc.console.param.log;

import com.ctrip.framework.drc.core.http.PageReq;

/**
 * Created by dengquanliang
 * 2023/10/31 11:15
 */
public class ConflictApprovalQueryParam {
    private String dbName;
    private String tableName;
    private String applicant;
    private Integer approvalResult;
    private PageReq pageReq;

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

    public String getApplicant() {
        return applicant;
    }

    public void setApplicant(String applicant) {
        this.applicant = applicant;
    }

    public Integer getApprovalResult() {
        return approvalResult;
    }

    public void setApprovalResult(Integer approvalResult) {
        this.approvalResult = approvalResult;
    }

    public PageReq getPageReq() {
        return pageReq;
    }

    public void setPageReq(PageReq pageReq) {
        this.pageReq = pageReq;
    }
}
