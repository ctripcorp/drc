package com.ctrip.framework.drc.messenger.activity.monitor;

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
    private String metricName;
    private String registryKey;
    private String mqType;
    private boolean send;

    public MqMonitorContext(int value, String registryKey, String metricName) {
        this.value = value;
        this.registryKey = registryKey;
        this.metricName = metricName;
    }

    public MqMonitorContext(String dbName, String tableName, int value, EventType eventType, DcTag dcTag, String topic, boolean send) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.value = value;
        this.eventType = eventType;
        this.dcTag = dcTag;
        this.topic = topic;
        this.send = send;
    }

    public MqMonitorContext(String dbName, String tableName, int value, EventType eventType, DcTag dcTag, String topic, String mqType, boolean send) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.value = value;
        this.eventType = eventType;
        this.dcTag = dcTag;
        this.topic = topic;
        this.mqType = mqType;
        this.send = send;
    }

    public String getMqType() {
        return mqType;
    }

    public void setMqType(String mqType) {
        this.mqType = mqType;
    }

    public boolean isSend() {
        return send;
    }

    public void setSend(boolean send) {
        this.send = send;
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

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public String getRegistryKey() {
        return registryKey;
    }

    public void setRegistryKey(String registryKey) {
        this.registryKey = registryKey;
    }
}
