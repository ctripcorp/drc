package com.ctrip.framework.drc.core.service.ops;

import java.util.Objects;

/**
 * @ClassName AppNode
 * @Author haodongPan
 * @Date 2022/3/17 19:54
 * @Version: $
 */
public class AppNode {

    private String ip;

    private String idc;

    private int port = 8080;

    public boolean isLegal() {
        return null != ip && !"".equals(ip);
    }

    @Override
    public String toString() {
        return "AppNode{" +
                "ip='" + ip + '\'' +
                ", idc='" + idc + '\'' +
                ", port=" + port +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AppNode appNode = (AppNode) o;
        return port == appNode.port && Objects.equals(ip, appNode.ip) && Objects.equals(idc, appNode.idc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, idc, port);
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

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}