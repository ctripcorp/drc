package com.ctrip.framework.drc.console.param.v2;

/**
 * Created by dengquanliang
 * 2023/7/27 14:53
 */
public class MessengerMhaBuildParam {
    private String mhaName;
    private String dc;
    private String buName;
    private String tag;

    @Override
    public String toString() {
        return "MessengerMhaBuildParam{" +
                "mhaName='" + mhaName + '\'' +
                ", dc='" + dc + '\'' +
                ", buName='" + buName + '\'' +
                ", tag='" + tag + '\'' +
                '}';
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
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

    public String getBuName() {
        return buName;
    }

    public void setBuName(String buName) {
        this.buName = buName;
    }
}
