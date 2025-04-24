package com.ctrip.framework.drc.console.vo.v2;

import java.util.List;

/**
 * Created by shiruixin
 * 2025/4/7 10:12
 */
public class MqMetaCreateResultView {
    int containTables;
    List<String> dbs;
    String region;
    String table;
    String topic;

    public int getContainTables() {
        return containTables;
    }

    public void setContainTables(int containTables) {
        this.containTables = containTables;
    }

    public List<String> getDbs() {
        return dbs;
    }

    public void setDbs(List<String> dbs) {
        this.dbs = dbs;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public String toString() {
        return "MqMetaCreateResultView{" +
                "containTables=" + containTables +
                ", dbs=" + dbs +
                ", region='" + region + '\'' +
                ", table='" + table + '\'' +
                ", topic='" + topic + '\'' +
                '}';
    }
}
