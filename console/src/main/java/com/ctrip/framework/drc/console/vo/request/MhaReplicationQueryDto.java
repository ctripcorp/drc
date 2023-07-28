package com.ctrip.framework.drc.console.vo.request;

import com.ctrip.framework.drc.core.http.PageReq;

import java.io.Serializable;


public class MhaReplicationQueryDto extends PageReq implements Serializable {
    private MhaQueryDto srcMha;
    private MhaQueryDto dstMha;

    public MhaQueryDto getSrcMha() {
        return srcMha;
    }

    public void setSrcMha(MhaQueryDto srcMha) {
        this.srcMha = srcMha;
    }

    public MhaQueryDto getDstMha() {
        return dstMha;
    }

    public void setDstMha(MhaQueryDto dstMha) {
        this.dstMha = dstMha;
    }

    @Override
    public String toString() {
        return "MhaReplicationQueryDto{" +
                "srcMha=" + srcMha +
                ", dstMha=" + dstMha +
                "} " + super.toString();
    }
}
