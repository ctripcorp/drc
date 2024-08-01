package com.ctrip.framework.drc.applier.activity.monitor;

import com.ctrip.framework.drc.core.mq.DcTag;
import com.ctrip.framework.drc.core.mq.EventType;

/**
 * Created by jixinwang on 2022/10/25
 */
public class MqMonitorContext {

    private String dbName;
    private String tableName;
    private int value;
    private EventType eventType;
    private DcTag dcTag;
    private String topic;

    public MqMonitorContext(String dbName, String tableName, int value, EventType eventType, DcTag dcTag, String topic) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.value = value;
        this.eventType = eventType;
        this.dcTag = dcTag;
        this.topic = topic;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public DcTag getDcTag() {
        return dcTag;
    }

    public void setDcTag(DcTag dcTag) {
        this.dcTag = dcTag;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
