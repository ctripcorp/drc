package com.ctrip.framework.drc.console.param.filter;

/**
 * Created by dengquanliang
 * 2023/4/24 14:32
 */
public class QConfigQueryParam {

    private String groupId;
    private String token;
    private String dataId;
    private String env;
    private String targetGroupId;
    private String subEnv;

    @Override
    public String toString() {
        return "QConfigQueryParam{" +
                "groupId='" + groupId + '\'' +
                ", token='" + token + '\'' +
                ", dataId='" + dataId + '\'' +
                ", env='" + env + '\'' +
                ", targetGroupId='" + targetGroupId + '\'' +
                ", subEnv='" + subEnv + '\'' +
                '}';
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getDataId() {
        return dataId;
    }

    public void setDataId(String dataId) {
        this.dataId = dataId;
    }

    public String getEnv() {
        return env;
    }

    public void setEnv(String env) {
        this.env = env;
    }

    public String getTargetGroupId() {
        return targetGroupId;
    }

    public void setTargetGroupId(String targetGroupId) {
        this.targetGroupId = targetGroupId;
    }

    public String getSubEnv() {
        return subEnv;
    }

    public void setSubEnv(String subEnv) {
        this.subEnv = subEnv;
    }
}
