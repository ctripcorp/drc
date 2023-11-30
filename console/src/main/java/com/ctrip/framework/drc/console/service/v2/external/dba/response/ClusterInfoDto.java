package com.ctrip.framework.drc.console.service.v2.external.dba.response;

import java.util.List;

public class ClusterInfoDto {
    /**
     * mha name
     */
    private String clusterName;
    private List<Node> nodes;
    /**
     * pro
     */
    private String env;
    /**
     * SHARB, sin-aws, fra-aws
     */
    private String zoneId;


    public static class Node {
        private String ipBusiness;
        private int instancePort;
        /**
         * SHA-ALI, SHAXY, SHARB, sin-aws, fra-aws
         */
        private String instanceZoneId;

        /**
         * master,slave,slave-dr
         */
        private String role;

        public String getIpBusiness() {
            return ipBusiness;
        }

        public void setIpBusiness(String ipBusiness) {
            this.ipBusiness = ipBusiness;
        }

        public int getInstancePort() {
            return instancePort;
        }

        public void setInstancePort(int instancePort) {
            this.instancePort = instancePort;
        }

        public String getInstanceZoneId() {
            return instanceZoneId;
        }

        public void setInstanceZoneId(String instanceZoneId) {
            this.instanceZoneId = instanceZoneId;
        }

        public String getRole() {
            return role;
        }

        public void setRole(String role) {
            this.role = role;
        }
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public List<Node> getNodes() {
        return nodes;
    }

    public void setNodes(List<Node> nodes) {
        this.nodes = nodes;
    }

    public String getEnv() {
        return env;
    }

    public void setEnv(String env) {
        this.env = env;
    }

    public String getZoneId() {
        return zoneId;
    }

    public void setZoneId(String zoneId) {
        this.zoneId = zoneId;
    }
}
