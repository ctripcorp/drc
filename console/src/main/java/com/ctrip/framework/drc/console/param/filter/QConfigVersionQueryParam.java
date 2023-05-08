package com.ctrip.framework.drc.console.param.filter;

/**
 * Created by dengquanliang
 * 2023/4/24 17:20
 */
public class QConfigVersionQueryParam extends QConfigBaseParam {

    private String targetEnv;
    private String targetSubEnv;
    private String targetGroupId;
    // separate with ,
    private String targetDataId;

    @Override
    public String toString() {
        return "QConfigVersionQueryParam{" +
                "targetEnv='" + targetEnv + '\'' +
                ", targetSubEnv='" + targetSubEnv + '\'' +
                ", targetGroupId='" + targetGroupId + '\'' +
                ", targetDataId='" + targetDataId + '\'' +
                '}';
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

}
