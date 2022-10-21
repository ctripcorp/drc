package com.ctrip.framework.drc.core.meta;

/**
 * @ClassName MqConfig
 * @Author haodongPan
 * @Date 2022/10/8 16:16
 * @Version: $
 */
public class MqConfig {
    // qmq/kafka
    private String mqType;
    //schema.table
    private String table;
    // mq topic
    private String topic;
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
    // for otter eventProcessor
    private String processor;

    @Override
    public String toString() {
        return "MqConfig{" +
                "mqType='" + mqType + '\'' +
                ", table='" + table + '\'' +
                ", topic='" + topic + '\'' +
                ", serialization='" + serialization + '\'' +
                ", persistent=" + persistent +
                ", persistentDb='" + persistentDb + '\'' +
                ", order=" + order +
                ", orderKey='" + orderKey + '\'' +
                ", delayTime=" + delayTime +
                ", processor='" + processor + '\'' +
                '}';
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

    public void setDelayTime(int delayTime) {
        this.delayTime = delayTime;
    }

    public String getProcessor() {
        return processor;
    }

    public void setProcessor(String processor) {
        this.processor = processor;
    }
}
