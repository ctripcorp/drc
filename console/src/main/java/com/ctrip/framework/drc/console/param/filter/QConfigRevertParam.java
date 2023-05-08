package com.ctrip.framework.drc.console.param.filter;

/**
 * Created by dengquanliang
 * 2023/4/28 17:40
 */
public class QConfigRevertParam extends QConfigBaseParam {
    private String targetGroupId;
    private String targetEnv;
    private String targetSubEnv;
    private String targetDataId;
    private int version;

    @Override
    public String toString() {
        return "QConfigRevertParam{" +
                "targetGroupId='" + targetGroupId + '\'' +
                ", targetEnv='" + targetEnv + '\'' +
                ", targetSubEnv='" + targetSubEnv + '\'' +
                ", targetDataId='" + targetDataId + '\'' +
                ", version=" + version +
                '}';
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

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }
}
