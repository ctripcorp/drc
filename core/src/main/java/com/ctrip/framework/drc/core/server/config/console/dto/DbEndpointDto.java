package com.ctrip.framework.drc.core.server.config.console.dto;

import java.util.Objects;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-03-04
 */
public class DbEndpointDto {

    private String ip;

    private int port;

    public DbEndpointDto(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public DbEndpointDto() {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DbEndpointDto)) return false;
        DbEndpointDto that = (DbEndpointDto) o;
        return getPort() == that.getPort() &&
                Objects.equals(getIp(), that.getIp());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getIp(), getPort());
    }

    @Override
    public String toString() {
        return "DbEndpointDto{" +
                "ip='" + ip + '\'' +
                ", port=" + port +
                '}';
    }
}
