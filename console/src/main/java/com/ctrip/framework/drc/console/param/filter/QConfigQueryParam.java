package com.ctrip.framework.drc.console.param.filter;

/**
 * Created by dengquanliang
 * 2023/4/24 14:32
 */
public class QConfigQueryParam extends QConfigBaseParam {

    private String dataId;
    private String env;
    private String targetGroupId;
    private String subEnv;

    @Override
    public String toString() {
        return "QConfigQueryParam{" +
                "dataId='" + dataId + '\'' +
                ", env='" + env + '\'' +
                ", targetGroupId='" + targetGroupId + '\'' +
                ", subEnv='" + subEnv + '\'' +
                '}';
    }

    public String getSubEnv() {
        return subEnv;
    }

    public void setSubEnv(String subEnv) {
        this.subEnv = subEnv;
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
}
