package com.ctrip.framework.drc.applier.mq;

/**
 * Created by jixinwang on 2022/10/17
 */
public class MqMediaSource {

    private String url;
    private String storePath;
    private MqType mqType;
    private String topic;
    private boolean persist;
    private String persistTitanKey;
    private String partitionKeys;
    private String serialization = "avro";
    private int delayTime = 0;           // qmq延迟消息时间，单位秒

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getStorePath() {
        return storePath;
    }

    public void setStorePath(String storePath) {
        this.storePath = storePath;
    }

    public MqType getMqType() {
        return mqType;
    }

    public void setMqType(MqType mqType) {
        this.mqType = mqType;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public boolean isPersist() {
        return persist;
    }

    public void setPersist(boolean persist) {
        this.persist = persist;
    }

    public String getPersistTitanKey() {
        return persistTitanKey;
    }

    public void setPersistTitanKey(String persistTitanKey) {
        this.persistTitanKey = persistTitanKey;
    }

    public String getPartitionKeys() {
        return partitionKeys;
    }

    public void setPartitionKeys(String partitionKeys) {
        this.partitionKeys = partitionKeys;
    }

    public String getSerialization() {
        return serialization;
    }

    public void setSerialization(String serialization) {
        this.serialization = serialization;
    }

    public int getDelayTime() {
        return delayTime;
    }

    public void setDelayTime(int delayTime) {
        this.delayTime = delayTime;
    }
}
