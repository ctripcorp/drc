package com.ctrip.framework.drc.core.server.container;

/**
 * Created by mingdongli
 * 2019/11/26 下午11:13.
 */
public class ZookeeperValue {

    private String ip;

    private int port;

    private String clusterName;

    private int status;

    public ZookeeperValue() {
    }

    public ZookeeperValue(String ip, int port, String clusterName, int status) {
        this.ip = ip;
        this.port = port;
        this.clusterName = clusterName;
        this.status = status;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }
}
