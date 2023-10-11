package com.ctrip.framework.drc.console.param.v2;

import com.ctrip.framework.drc.core.http.PageReq;

public class DbQuery extends PageReq {
    private String likeByDbNameFromBeginning;

    public String getLikeByDbNameFromBeginning() {
        return likeByDbNameFromBeginning;
    }

    public void setLikeByDbNameFromBeginning(String likeByDbNameFromBeginning) {
        this.likeByDbNameFromBeginning = likeByDbNameFromBeginning;
    }
}
