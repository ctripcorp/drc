package com.ctrip.framework.drc.console.param.filter;

/**
 * Created by dengquanliang
 * 2023/4/25 10:43
 */
public class QConfigBatchUpdateParam extends QConfigBaseParam {

    private String targetGroupId;
    private String targetEnv;
    private String targetSubEnv;
    private String targetDataId;
    private QConfigBatchUpdateDetailParam detailParam;

    @Override
    public String toString() {
        return "QConfigBatchUpdateParam{" +
                "targetGroupId='" + targetGroupId + '\'' +
                ", targetEnv='" + targetEnv + '\'' +
                ", targetSubEnv='" + targetSubEnv + '\'' +
                ", targetDataId='" + targetDataId + '\'' +
                ", detailParam=" + detailParam +
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

    public QConfigBatchUpdateDetailParam getDetailParam() {
        return detailParam;
    }

    public void setDetailParam(QConfigBatchUpdateDetailParam detailParam) {
        this.detailParam = detailParam;
    }
}
