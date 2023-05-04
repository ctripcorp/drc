package com.ctrip.framework.drc.console.vo.filter;

/**
 * Created by dengquanliang
 * 2023/4/24 14:36
 */
public class QConfigDetailData {

    private String group;
    private String dataId;
    private String profile;
    private int basedVersion;
    private int editVersion;
    private String data;
    private String operator;
    private String status;
    private String updateTime;

    public QConfigDetailData(String msg) {

    }

    public QConfigDetailData() {}

    @Override
    public String toString() {
        return "QConfigDetailData{" +
                "group='" + group + '\'' +
                ", dataId='" + dataId + '\'' +
                ", profile='" + profile + '\'' +
                ", basedVersion=" + basedVersion +
                ", editVersion=" + editVersion +
                ", data='" + data + '\'' +
                ", operator='" + operator + '\'' +
                ", status='" + status + '\'' +
                ", updateTime='" + updateTime + '\'' +
                '}';
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getDataId() {
        return dataId;
    }

    public void setDataId(String dataId) {
        this.dataId = dataId;
    }

    public String getProfile() {
        return profile;
    }

    public void setProfile(String profile) {
        this.profile = profile;
    }

    public int getBasedVersion() {
        return basedVersion;
    }

    public void setBasedVersion(int basedVersion) {
        this.basedVersion = basedVersion;
    }

    public int getEditVersion() {
        return editVersion;
    }

    public void setEditVersion(int editVersion) {
        this.editVersion = editVersion;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }
}
