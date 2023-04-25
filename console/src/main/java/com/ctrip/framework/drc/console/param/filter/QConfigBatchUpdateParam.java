package com.ctrip.framework.drc.console.param.filter;

/**
 * Created by dengquanliang
 * 2023/4/25 10:43
 */
public class QConfigBatchUpdateParam {

    private String token;
    private String targetGroupId;
    private String targetEnv;
    private String targetSubEnv;
    private String targetDataId;
    private String serverEnv;
    private String groupId;
    private String operator;
    private QConfigBatchUpdateDetailParam detailParam;

    @Override
    public String toString() {
        return "QConfigBatchUpdateParam{" +
                "token='" + token + '\'' +
                ", targetGroupId='" + targetGroupId + '\'' +
                ", targetEnv='" + targetEnv + '\'' +
                ", targetSubEnv='" + targetSubEnv + '\'' +
                ", targetDataId='" + targetDataId + '\'' +
                ", serverEnv='" + serverEnv + '\'' +
                ", groupId='" + groupId + '\'' +
                ", operator='" + operator + '\'' +
                ", detailParam=" + detailParam +
                '}';
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getTargetGroupId() {
        return targetGroupId;
    }

    public void setTargetGroupId(String targetGroupId) {
        this.targetGroupId = targetGroupId;
    }

    public String getTargetEnv() {
        return targetEnv;
    }

    public void setTargetEnv(String targetEnv) {
        this.targetEnv = targetEnv;
    }

    public String getTargetSubEnv() {
        return targetSubEnv;
    }

    public void setTargetSubEnv(String targetSubEnv) {
        this.targetSubEnv = targetSubEnv;
    }

    public String getTargetDataId() {
        return targetDataId;
    }

    public void setTargetDataId(String targetDataId) {
        this.targetDataId = targetDataId;
    }

    public String getServerEnv() {
        return serverEnv;
    }

    public void setServerEnv(String serverEnv) {
        this.serverEnv = serverEnv;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public QConfigBatchUpdateDetailParam getDetailParam() {
        return detailParam;
    }

    public void setDetailParam(QConfigBatchUpdateDetailParam detailParam) {
        this.detailParam = detailParam;
    }
}
