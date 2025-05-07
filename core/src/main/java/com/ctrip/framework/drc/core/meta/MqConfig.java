package com.ctrip.framework.drc.core.meta;

import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * @ClassName MqConfig
 * @Author haodongPan
 * @Date 2022/10/8 16:16
 * @Version: $
 */
public class MqConfig {
    
    //schema.table,store at dataMediaPair
    private String table;
    // mq topic,store at dataMediaPair
    private String topic;
    // for otter eventProcessor,store at dataMediaPair
    private String processor;
    
    // qmq/kafka
    private String mqType;
    //json/arvo
    private String serialization;
    // qmq store message when send fail
    private boolean persistent;
    // titankey/dalCluster
    private String persistentDb;
    // partition order
    private boolean order;
    // orderKey / partition key
    private String orderKey;
    // qmq send message delayTime, unit:second
    private long delayTime;
    // send message for target subenv
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String subenv;
    //EventType value: I, U, D
    private List<String> excludeFilterTypes;
    private List<String> filterFields;
    private boolean sendOnlyUpdated;
    private boolean excludeColumn;

    @Override
    public String toString() {
        return "MqConfig{" +
                "table='" + table + '\'' +
                ", topic='" + topic + '\'' +
                ", processor='" + processor + '\'' +
                ", mqType='" + mqType + '\'' +
                ", serialization='" + serialization + '\'' +
                ", persistent=" + persistent +
                ", persistentDb='" + persistentDb + '\'' +
                ", order=" + order +
                ", orderKey='" + orderKey + '\'' +
                ", delayTime=" + delayTime +
                ", subenv='" + subenv + '\'' +
                ", excludeFilterTypes=" + excludeFilterTypes +
                ", filterFields=" + filterFields +
                ", sendOnlyUpdated=" + sendOnlyUpdated +
                ", excludeColumn=" + excludeColumn +
                '}';
    }

    public List<String> getExcludeFilterTypes() {
        return excludeFilterTypes;
    }

    public void setExcludeFilterTypes(List<String> excludeFilterTypes) {
        this.excludeFilterTypes = excludeFilterTypes;
    }

    public String getMqType() {
        return mqType;
    }

    public void setMqType(String mqType) {
        this.mqType = mqType;
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

    public String getSerialization() {
        return serialization;
    }

    public void setSerialization(String serialization) {
        this.serialization = serialization;
    }

    public boolean isPersistent() {
        return persistent;
    }

    public void setPersistent(boolean persistent) {
        this.persistent = persistent;
    }

    public String getPersistentDb() {
        return persistentDb;
    }

    public void setPersistentDb(String persistentDb) {
        this.persistentDb = persistentDb;
    }

    public boolean isOrder() {
        return order;
    }

    public void setOrder(boolean order) {
        this.order = order;
    }

    public String getOrderKey() {
        return orderKey;
    }

    public void setOrderKey(String orderKey) {
        this.orderKey = orderKey;
    }

    public long getDelayTime() {
        return delayTime;
    }

    public void setDelayTime(long delayTime) {
        this.delayTime = delayTime;
    }

    public String getProcessor() {
        return processor;
    }

    public void setProcessor(String processor) {
        this.processor = processor;
    }

    public String getSubenv() {
        return subenv;
    }

    public void setSubenv(String subenv) {
        this.subenv = subenv;
    }

    public String toJson() {
        return JsonUtils.toJson(this);
    }

    public List<String> getFilterFields() {
        return filterFields;
    }

    public void setFilterFields(List<String> filterFields) {
        this.filterFields = filterFields;
    }

    public boolean isSendOnlyUpdated() {
        return sendOnlyUpdated;
    }

    public void setSendOnlyUpdated(boolean sendOnlyUpdated) {
        this.sendOnlyUpdated = sendOnlyUpdated;
    }

    public boolean isExcludeColumn() {
        return excludeColumn;
    }

    public void setExcludeColumn(boolean excludeColumn) {
        this.excludeColumn = excludeColumn;
    }
}
