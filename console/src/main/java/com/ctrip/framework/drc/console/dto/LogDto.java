package com.ctrip.framework.drc.console.dto;

/**
 * Created by jixinwang on 2020/6/22
 */
public class LogDto {

    private String srcDcName;

    private String destDcName;

    private String clusterName;

    private String userId;

    private String sqlStatement;

    private String sqlHandleConflict;

    public LogDto() {
    }

    public LogDto(String srcDcName, String destDcName, String clusterName, String userId, String sqlStatement, String sqlHandleConflict) {
        this.srcDcName = srcDcName;
        this.destDcName = destDcName;
        this.clusterName = clusterName;
        this.userId = userId;
        this.sqlStatement = sqlStatement;
        this.sqlHandleConflict = sqlHandleConflict;
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

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getSqlStatement() {
        return sqlStatement;
    }

    public void setSqlStatement(String sqlStatement) {
        this.sqlStatement = sqlStatement;
    }

    public String getSqlHandleConflict() {
        return sqlHandleConflict;
    }

    public void setSqlHandleConflict(String sqlHandleConflict) {
        this.sqlHandleConflict = sqlHandleConflict;
    }
}
