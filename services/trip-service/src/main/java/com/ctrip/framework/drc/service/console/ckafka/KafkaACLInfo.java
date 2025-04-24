package com.ctrip.framework.drc.service.console.ckafka;

import java.util.List;

/**
 * Created by shiruixin
 * 2025/4/7 15:52
 */
public class KafkaACLInfo {
    private String topic;
    private String clusterSet;
    private String cluster;
    private String zone;
    private List<String> type;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getClusterSet() {
        return clusterSet;
    }

    public void setClusterSet(String clusterSet) {
        this.clusterSet = clusterSet;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public List<String> getType() {
        return type;
    }

    public void setType(List<String> type) {
        this.type = type;
    }
}
