package com.ctrip.framework.drc.console.dto;

/**
 * @ClassName MqConfigDto
 * @Author haodongPan
 * @Date 2022/10/26 10:42
 * @Version: $
 */
public class MqConfigDto {
    
    private long id;
    
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
    private String mhaName;
    
    // mark topic when one table match many topic
    private String tag;

    @Override
    public String toString() {
        return "MqConfigDto{" +
                "id=" + id +
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
                ", mhaName='" + mhaName + '\'' +
                ", tag='" + tag + '\'' +
                '}';
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
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

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }
}
