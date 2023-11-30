package com.ctrip.framework.drc.console.vo.check.v2;


public class MqConfigConflictTable {

    private String table;

    private String topic;


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
        return "MqConfigConflictTable{" +
                "table='" + table + '\'' +
                ", topic='" + topic + '\'' +
                '}';
    }
}
