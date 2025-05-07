package com.ctrip.framework.drc.console.vo.display.v2;


import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import com.ctrip.framework.drc.core.mq.MqType;

import java.util.List;

public class MqConfigVo {

    /**
     * @see DbReplicationTbl
     */
    private long dbReplicationId;

    /**
     * @see MqType
     */
    private String mqType;

    /**
     * schema.table
     */
    private String table;

    /**
     * mq topic
     */
    private String topic;

    /**
     * json/arvo
     */
    private String serialization;

    /**
     * qmq store message when send fail
     */
    private boolean persistent;

    /**
     * partition order
     */
    private boolean order;

    /**
     * orderKey / partition key
     */
    private String orderKey;
    /**
     * qmq send message delayTime, unit:second
     */
    private long delayTime;

    private Long datachangeLasttime;

    private List<String> excludeFilterTypes;

    private List<String> filterFields;

    private boolean sendOnlyUpdated;

    private boolean excludeColumn;

    public List<String> getExcludeFilterTypes() {
        return excludeFilterTypes;
    }

    public void setExcludeFilterTypes(List<String> excludeFilterTypes) {
        this.excludeFilterTypes = excludeFilterTypes;
    }

    public long getDbReplicationId() {
        return dbReplicationId;
    }

    public void setDbReplicationId(long dbReplicationId) {
        this.dbReplicationId = dbReplicationId;
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

    public Long getDatachangeLasttime() {
        return datachangeLasttime;
    }

    public void setDatachangeLasttime(Long datachangeLasttime) {
        this.datachangeLasttime = datachangeLasttime;
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
