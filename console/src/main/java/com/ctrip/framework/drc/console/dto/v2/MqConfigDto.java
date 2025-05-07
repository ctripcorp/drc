package com.ctrip.framework.drc.console.dto.v2;

import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.framework.drc.core.meta.MqConfig;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.springframework.util.CollectionUtils;

import java.io.Serializable;
import java.util.List;

public class MqConfigDto implements Serializable {

    private List<Long> dbReplicationIds;
    private String mhaName;
    private String bu;
    private String mqType;
    private String table;
    private String topic;
    private String serialization;
    private boolean persistent;
    private String persistentDb;
    private boolean order;
    private String orderKey;
    private long delayTime;
    private String processor;
    private long messengerGroupId;
    //EventType value: I, U, D
    private List<String> excludeFilterTypes;
    private List<String> filterFields;
    private boolean sendOnlyUpdated;
    private boolean excludeColumn;

    public void validCheckRequest() {
        if (StringUtils.isBlank(mhaName)) {
            throw new IllegalArgumentException("mhaName is empty!");
        }
        if (StringUtils.isBlank(table)) {
            throw new IllegalArgumentException("please input table!");
        }
        if(MqType.parse(mqType) == null) {
            throw new IllegalArgumentException("mqType is invalid! mqType: " + mqType);
        }
    }

    public boolean isInsertRequest() {
        return CollectionUtils.isEmpty(this.getDbReplicationIds());
    }

    public Long getDbReplicationId() {
        if (CollectionUtils.isEmpty(dbReplicationIds)) {
            return null;
        }
        if (dbReplicationIds.size() > 1) {
            throw new IllegalArgumentException("more than one db replication id!");
        }
        return dbReplicationIds.get(0);
    }

    public List<Long> getDbReplicationIds() {
        return dbReplicationIds;
    }

    public void setDbReplicationIds(List<Long> dbReplicationIds) {
        this.dbReplicationIds = dbReplicationIds;
    }

    public void setDbReplicationId(Long dbReplicationId) {
        if (dbReplicationId == null) {
            return;
        }
        this.dbReplicationIds = Lists.newArrayList(dbReplicationId);
    }

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public String getBu() {
        return bu;
    }

    public void setBu(String bu) {
        this.bu = bu;
    }

    public String getMqType() {
        return mqType;
    }
    public MqType getMqTypeEnum(){
        return MqType.parseOrDefault(mqType);
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

    public long getMessengerGroupId() {
        return messengerGroupId;
    }

    public void setMessengerGroupId(long messengerGroupId) {
        this.messengerGroupId = messengerGroupId;
    }

    public List<String> getExcludeFilterTypes() {
        return excludeFilterTypes;
    }

    public void setExcludeFilterTypes(List<String> excludeFilterTypes) {
        this.excludeFilterTypes = excludeFilterTypes;
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

    public boolean isExcludeColumn() {
        return excludeColumn;
    }

    @Override
    public String toString() {
        return "MqConfigDto{" +
                "dbReplicationIds=" + dbReplicationIds +
                ", mhaName='" + mhaName + '\'' +
                ", bu='" + bu + '\'' +
                ", mqType='" + mqType + '\'' +
                ", table='" + table + '\'' +
                ", topic='" + topic + '\'' +
                ", serialization='" + serialization + '\'' +
                ", persistent=" + persistent +
                ", persistentDb='" + persistentDb + '\'' +
                ", order=" + order +
                ", orderKey='" + orderKey + '\'' +
                ", delayTime=" + delayTime +
                ", processor='" + processor + '\'' +
                ", messengerGroupId=" + messengerGroupId +
                ", excludeFilterTypes=" + excludeFilterTypes +
                ", filterFields=" + filterFields +
                ", sendOnlyUpdated=" + sendOnlyUpdated +
                ", excludeColumn=" + excludeColumn +
                '}';
    }


    public MqConfig build() {
        MqConfig mqConfig = new MqConfig();
        mqConfig.setMqType(this.getMqType());
        mqConfig.setSerialization(this.getSerialization());
        mqConfig.setOrder(this.isOrder());
        if (this.isOrder()) {
            mqConfig.setOrderKey(this.getOrderKey());
        }
        mqConfig.setPersistent(this.isPersistent());
        mqConfig.setPersistentDb(this.getPersistentDb());
        mqConfig.setDelayTime(this.getDelayTime());
        if (!CollectionUtils.isEmpty(this.getExcludeFilterTypes())) {
            mqConfig.setExcludeFilterTypes(this.getExcludeFilterTypes());
        }
        if (!CollectionUtils.isEmpty(this.getFilterFields())) {
            mqConfig.setFilterFields(this.getFilterFields());
        }
        mqConfig.setSendOnlyUpdated(this.isSendOnlyUpdated());
        mqConfig.setExcludeColumn(this.isExcludeColumn());

        return mqConfig;
    }

}
