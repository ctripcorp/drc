package com.ctrip.framework.drc.console.param.mysql;

import java.util.List;

public class DrcDbMonitorTableCreateReq {
    private String mha;
    private List<String> dbs;

    public DrcDbMonitorTableCreateReq() {
    }

    public DrcDbMonitorTableCreateReq(String mha, List<String> dbs) {
        this.mha = mha;
        this.dbs = dbs;
    }

    public String getMha() {
        return mha;
    }

    public void setMha(String mha) {
        this.mha = mha;
    }


    public List<String> getDbs() {
        return dbs;
    }

    public void setDbs(List<String> dbs) {
        this.dbs = dbs;
    }
}
