package com.ctrip.framework.drc.console.monitor.delay.config;

import java.util.Objects;

/**
 * @ClassName FullDataConsistencyCheckTestConfig
 * @Author haodongPan
 * @Date 2021/9/1 15:58
 * @Version: $
 */
public class FullDataConsistencyCheckTestConfig extends DelayMonitorConfig {

    private String ipA;

    private int portA;

    private String userA;

    private String passwordA;

    private String ipB;

    private int portB;

    private String userB;

    private String passwordB;

    private String startTimestamp;

    private String endTimeStamp;


    public String getIpA() {
        return ipA;
    }

    public void setIpA(String ipA) {
        this.ipA = ipA;
    }

    public int getPortA() {
        return portA;
    }

    public void setPortA(int portA) {
        this.portA = portA;
    }

    public String getUserA() {
        return userA;
    }

    public void setUserA(String userA) {
        this.userA = userA;
    }

    public String getPasswordA() {
        return passwordA;
    }

    public void setPasswordA(String passwordA) {
        this.passwordA = passwordA;
    }


    public String getIpB() {
        return ipB;
    }

    public void setIpB(String ipB) {
        this.ipB = ipB;
    }

    public int getPortB() {
        return portB;
    }

    public void setPortB(int portB) {
        this.portB = portB;
    }

    public String getUserB() {
        return userB;
    }

    public void setUserB(String userB) {
        this.userB = userB;
    }

    public String getPasswordB() {
        return passwordB;
    }

    public void setPasswordB(String passwordB) {
        this.passwordB = passwordB;
    }

    public String getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(String startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public String getEndTimeStamp() {
        return endTimeStamp;
    }

    public void setEndTimeStamp(String endTimeStamp) {
        this.endTimeStamp = endTimeStamp;
    }

    @Override
    public String toString() {
        return "FullDataConsistencyCheckTestConfig{" +
                "ipA='" + ipA + '\'' +
                ", portA=" + portA +
                ", userA='" + userA + '\'' +
                ", passwordA='" + passwordA + '\'' +
                ", ipB='" + ipB + '\'' +
                ", portB=" + portB +
                ", userB='" + userB + '\'' +
                ", passwordB='" + passwordB + '\'' +
                ", startTimestamp='" + startTimestamp + '\'' +
                ", endTimeStamp='" + endTimeStamp + '\'' +
                '}';
    }
}
