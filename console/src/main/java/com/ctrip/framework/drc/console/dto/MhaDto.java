package com.ctrip.framework.drc.console.dto;

/**
 * @ClassName MhaDto
 * @Author haodongPan
 * @Date 2022/10/25 15:49
 * @Version: $
 */
public class MhaDto {
    
    private String buName;

    private String dalClusterName;

    private Long appid;

    private String mhaName;

    private String dc;
    
    private int monitorSwitch;

    @Override
    public String toString() {
        return "MhaDto{" +
                "buName='" + buName + '\'' +
                ", dalClusterName='" + dalClusterName + '\'' +
                ", appid=" + appid +
                ", mhaName='" + mhaName + '\'' +
                ", dc='" + dc + '\'' +
                ", monitorSwitch=" + monitorSwitch +
                '}';
    }

    public String getBuName() {
        return buName;
    }

    public void setBuName(String buName) {
        this.buName = buName;
    }

    public String getDalClusterName() {
        return dalClusterName;
    }

    public void setDalClusterName(String dalClusterName) {
        this.dalClusterName = dalClusterName;
    }

    public Long getAppid() {
        return appid;
    }

    public void setAppid(Long appid) {
        this.appid = appid;
    }

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public String getDc() {
        return dc;
    }

    public void setDc(String dc) {
        this.dc = dc;
    }

    public int getMonitorSwitch() {
        return monitorSwitch;
    }

    public void setMonitorSwitch(int monitorSwitch) {
        this.monitorSwitch = monitorSwitch;
    }
}
