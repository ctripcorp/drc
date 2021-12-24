package com.ctrip.framework.drc.console.dto;

import java.util.Objects;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-22
 */
public class GtidFillDto {

    private String ip;

    private int port;

    private String gtidSet;

    private boolean test;

    public GtidFillDto() {
    }

    public GtidFillDto(String ip, int port, String gtidSet, boolean test) {
        this.ip = ip;
        this.port = port;
        this.gtidSet = gtidSet;
        this.test = test;
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

    public String getGtidSet() {
        return gtidSet;
    }

    public void setGtidSet(String gtidSet) {
        this.gtidSet = gtidSet;
    }

    public boolean isTest() {
        return test;
    }

    public void setTest(boolean test) {
        this.test = test;
    }

    @Override
    public String toString() {
        return "GtidFillDto{" +
                "ip='" + ip + '\'' +
                ", port=" + port +
                ", gtidSet='" + gtidSet + '\'' +
                ", test=" + test +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GtidFillDto)) return false;
        GtidFillDto that = (GtidFillDto) o;
        return getPort() == that.getPort() &&
                isTest() == that.isTest() &&
                getIp().equals(that.getIp()) &&
                getGtidSet().equals(that.getGtidSet());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getIp(), getPort(), getGtidSet(), isTest());
    }
}
