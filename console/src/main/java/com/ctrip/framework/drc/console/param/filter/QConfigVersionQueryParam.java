package com.ctrip.framework.drc.console.param.filter;

/**
 * Created by dengquanliang
 * 2023/4/24 17:20
 */
public class QConfigVersionQueryParam {

    private String token;
    private String targetEnv;
    private String targetSubEnv;
    private String targetGroupId;
    private String targetDataId;
    private String operator;
    private String serverEnv;
    private String groupId;

    @Override
    public String toString() {
        return "QConfigVersionQueryParam{" +
                "token='" + token + '\'' +
                ", targetEnv='" + targetEnv + '\'' +
                ", targetSubEnv='" + targetSubEnv + '\'' +
                ", targetGroupId='" + targetGroupId + '\'' +
                ", targetDataId='" + targetDataId + '\'' +
                ", operator='" + operator + '\'' +
                ", serverEnv='" + serverEnv + '\'' +
                ", groupId='" + groupId + '\'' +
                '}';
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
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

    public String getTargetGroupId() {
        return targetGroupId;
    }

    public void setTargetGroupId(String targetGroupId) {
        this.targetGroupId = targetGroupId;
    }

    public String getTargetDataId() {
        return targetDataId;
    }

    public void setTargetDataId(String targetDataId) {
        this.targetDataId = targetDataId;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
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
}
