package com.ctrip.framework.drc.console.dto;

/**
 * @ClassName QmqTopicDto
 * @Author haodongPan
 * @Date 2022/10/31 11:28
 * @Version: $
 */
public class QmqTopicDto {
    
    private String bu;
    private String topic;
    private String cluster;

    public String getBu() {
        return bu;
    }

    public void setBu(String bu) {
        this.bu = bu;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }
}
