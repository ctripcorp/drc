package com.ctrip.framework.drc.core.server.config.console.dto;

import java.util.Map;

/**
 * Created by dengquanliang
 * 2024/10/14 19:02
 */
public class ClusterConfigDto {
    private Map<String, String> clusterMap;

    private boolean firstHand;

    public ClusterConfigDto() {
    }

    public ClusterConfigDto(Map<String, String> clusterMap, boolean firstHand) {
        this.clusterMap = clusterMap;
        this.firstHand = firstHand;
    }

    public Map<String, String> getClusterMap() {
        return clusterMap;
    }

    public void setClusterMap(Map<String, String> clusterMap) {
        this.clusterMap = clusterMap;
    }

    public boolean isFirstHand() {
        return firstHand;
    }

    public void setFirstHand(boolean firstHand) {
        this.firstHand = firstHand;
    }

    @Override
    public String toString() {
        return "ClusterConfigDto{" +
                "clusterMap=" + clusterMap +
                ", firstHand=" + firstHand +
                '}';
    }
}
