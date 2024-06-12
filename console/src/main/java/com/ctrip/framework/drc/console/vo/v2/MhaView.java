package com.ctrip.framework.drc.console.vo.v2;

/**
 * Created by dengquanliang
 * 2024/6/4 15:19
 */
public class MhaView {
    private String mhaName;

    public MhaView() {
    }

    public MhaView(String mhaName) {
        this.mhaName = mhaName;
    }

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }
}
