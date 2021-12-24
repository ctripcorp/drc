package com.ctrip.framework.drc.core.meta;

import java.util.Objects;

/**
 * @Author Slight
 * Nov 07, 2019
 */
public class InstanceInfo {
    public String name;
    public String mhaName;
    public int port;
    public String ip;
    public String idc;
    public String cluster;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getIdc() {
        return idc;
    }

    public void setIdc(String idc) {
        this.idc = idc;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    @Override
    public String toString() {
        return "InstanceInfo{" +
                "name='" + name + '\'' +
                ", mhaName='" + mhaName + '\'' +
                ", port=" + port +
                ", ip='" + ip + '\'' +
                ", idc='" + idc + '\'' +
                ", cluster='" + cluster + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InstanceInfo)) return false;
        InstanceInfo that = (InstanceInfo) o;
        return port == that.port &&
                Objects.equals(name, that.name) &&
                Objects.equals(mhaName, that.mhaName) &&
                Objects.equals(ip, that.ip) &&
                Objects.equals(idc, that.idc) &&
                Objects.equals(cluster, that.cluster);
    }

    @Override
    public int hashCode() {

        return Objects.hash(name, mhaName, port, ip, idc, cluster);
    }
}
