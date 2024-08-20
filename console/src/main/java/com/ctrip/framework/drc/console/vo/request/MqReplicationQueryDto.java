package com.ctrip.framework.drc.console.vo.request;

import com.ctrip.framework.drc.core.http.PageReq;

import java.io.Serializable;

/**
 * Created by shiruixin
 * 2024/8/13 11:24
 */
public class MqReplicationQueryDto extends PageReq implements Serializable {
    private String dbNames;
    private String srcTblName;
    private String topic;
    private boolean queryOtter;

    public String getDbNames() {
        return dbNames;
    }

    public void setDbNames(String dbNames) {
        this.dbNames = dbNames;
    }

    public String getSrcTblName() {
        return srcTblName;
    }

    public void setSrcTblName(String srcTblName) {
        this.srcTblName = srcTblName;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public boolean isQueryOtter() {
        return queryOtter;
    }

    public void setQueryOtter(boolean queryOtter) {
        this.queryOtter = queryOtter;
    }

    public static MqReplicationQueryDto from(String dbNames, String srcTblName, String topic, boolean queryOtter) {
        MqReplicationQueryDto dto = new MqReplicationQueryDto();
        dto.setDbNames(dbNames);
        dto.setSrcTblName(srcTblName);
        dto.setTopic(topic);
        dto.setQueryOtter(queryOtter);
        return dto;
    }

    @Override
    public String toString() {
        return "DbReplicationQueryDto{" +
                "dbNames=" + dbNames +
                ", srcTblName=" + srcTblName +
                ", topic=" + topic +
                ", queryOtter=" + queryOtter +
                "} " + super.toString();
    }
}
