package com.ctrip.framework.drc.service.console.ckafka;

import java.util.List;

/**
 * Created by shiruixin
 * 2025/4/18 11:32
 */
public class KafkaValidateTopicResponse {
    private String status;
    List<KafkaClusterSetInfo> clusters;
    private String message;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public List<KafkaClusterSetInfo> getClusters() {
        return clusters;
    }

    public void setClusters(List<KafkaClusterSetInfo> clusters) {
        this.clusters = clusters;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
