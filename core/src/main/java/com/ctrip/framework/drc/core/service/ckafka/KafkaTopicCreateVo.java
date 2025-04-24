package com.ctrip.framework.drc.core.service.ckafka;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * Created by shiruixin
 * 2025/4/18 14:01
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class KafkaTopicCreateVo {
    @JsonIgnore
    public static final String ORIGIN = "100059182";

    String topic;
    String description;
    String clusterSet;
    String owner;
    String bu;
    String codec;
    Integer partitions;
    Integer maxMessageMB;
    List<String> region;
    String origin;

    public KafkaTopicCreateVo(String topic, String clusterSet, String owner, String bu,
                              Integer partitions, Integer maxMessageMB, String kafkaRegion) {
        this.topic = topic;
        this.clusterSet = clusterSet;
        this.owner = owner;
        this.bu = bu;
        this.codec = "json";
        this.partitions = partitions;
        this.maxMessageMB = maxMessageMB;
        this.region = List.of(kafkaRegion);
        this.origin = ORIGIN;
        this.description = "drc binlog message";
    }

    public KafkaTopicCreateVo(String topic, String kafkaRegion) {
        this.region = List.of(kafkaRegion);;
        this.topic = topic;
        this.origin = ORIGIN;
    }

    public KafkaTopicCreateVo(String topic, String clusterSet, String kafkaRegion) {
        this.topic = topic;
        this.clusterSet = clusterSet;
        this.region = List.of(kafkaRegion);
        this.origin = ORIGIN;
    }

    public KafkaTopicCreateVo () {
        this.origin = ORIGIN;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public List<String> getRegion() {
        return region;
    }

    public void setRegion(List<String> region) {
        this.region = region;
    }

    public Integer getMaxMessageMB() {
        return maxMessageMB;
    }

    public void setMaxMessageMB(Integer maxMessageMB) {
        this.maxMessageMB = maxMessageMB;
    }

    public Integer getPartitions() {
        return partitions;
    }

    public void setPartitions(Integer partitions) {
        this.partitions = partitions;
    }

    public String getCodec() {
        return codec;
    }

    public void setCodec(String codec) {
        this.codec = codec;
    }

    public String getBu() {
        return bu;
    }

    public void setBu(String bu) {
        this.bu = bu;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getClusterSet() {
        return clusterSet;
    }

    public void setClusterSet(String clusterSet) {
        this.clusterSet = clusterSet;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
