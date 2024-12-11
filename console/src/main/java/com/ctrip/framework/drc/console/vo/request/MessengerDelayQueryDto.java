package com.ctrip.framework.drc.console.vo.request;

import java.io.Serializable;
import java.util.List;


public class MessengerDelayQueryDto implements Serializable {
    private List<String> mhas;
    private List<String> dbs;
    private Boolean noNeedDbAndSrcTime;

    public void setNoNeedDbAndSrcTime(Boolean noNeedDbAndSrcTime) {
        this.noNeedDbAndSrcTime = noNeedDbAndSrcTime;
    }

    public boolean getNoNeedDbAndSrcTime() {
        return Boolean.TRUE.equals(noNeedDbAndSrcTime);
    }

    public List<String> getMhas() {
        return mhas;
    }

    public void setMhas(List<String> mhas) {
        this.mhas = mhas;
    }

    public List<String> getDbs() {
        return dbs;
    }

    public void setDbs(List<String> dbs) {
        this.dbs = dbs;
    }
}
