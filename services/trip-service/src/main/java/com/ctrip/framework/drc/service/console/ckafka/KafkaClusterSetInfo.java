package com.ctrip.framework.drc.service.console.ckafka;

/**
 * Created by shiruixin
 * 2025/4/18 11:33
 */
public class KafkaClusterSetInfo {
    private String clusterSet;
    private String region;
    private String zone;
    private String bootstrapServer;
    private String securityDomain;

    public String getClusterSet() {
        return clusterSet;
    }

    public void setClusterSet(String clusterSet) {
        this.clusterSet = clusterSet;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public String getBootstrapServer() {
        return bootstrapServer;
    }

    public void setBootstrapServer(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    public String getSecurityDomain() {
        return securityDomain;
    }

    public void setSecurityDomain(String securityDomain) {
        this.securityDomain = securityDomain;
    }
}
