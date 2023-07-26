package com.ctrip.framework.drc.console.vo.request;

import com.ctrip.framework.drc.core.http.PageReq;


public class DrcGroupQueryDto extends PageReq {
    /**
     * mha name(source)
     */
    private String srcMha;
    private String destMha;

    public String getSrcMha() {
        return srcMha;
    }

    public void setSrcMha(String srcMha) {
        this.srcMha = srcMha;
    }

    public String getDestMha() {
        return destMha;
    }

    public void setDestMha(String destMha) {
        this.destMha = destMha;
    }

    @Override
    public String toString() {
        return "DrcGroupQueryDto{" +
                "srcMha='" + srcMha + '\'' +
                ", destMha='" + destMha + '\'' +
                '}';
    }
}
