package com.ctrip.framework.drc.console.vo.display;

/**
 * @ClassName MessengerVo
 * @Author haodongPan
 * @Date 2022/11/2 17:27
 * @Version: $
 */
public class MessengerVo {
    private String mhaName;
    private String bu;
    private int monitorSwitch;

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public String getBu() {
        return bu;
    }

    public void setBu(String bu) {
        this.bu = bu;
    }

    public int getMonitorSwitch() {
        return monitorSwitch;
    }

    public void setMonitorSwitch(int monitorSwitch) {
        this.monitorSwitch = monitorSwitch;
    }
}
