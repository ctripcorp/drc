package com.ctrip.framework.drc.console.param.mysql;

public class DrcMessengerGtidTblCreateReq {
    private String mha;

    public DrcMessengerGtidTblCreateReq() {
    }

    public DrcMessengerGtidTblCreateReq(String mha) {
        this.mha = mha;
    }

    public String getMha() {
        return mha;
    }

    public void setMha(String mha) {
        this.mha = mha;
    }
}
