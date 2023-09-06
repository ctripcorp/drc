package com.ctrip.framework.drc.console.vo.request;

import com.ctrip.framework.drc.core.http.PageReq;

import java.io.Serializable;


public class MhaReplicationQueryDto extends PageReq implements Serializable {
    private MhaQueryDto srcMha;
    private MhaQueryDto dstMha;
    private MhaQueryDto relatedMha;
    private Integer drcStatus;

    public Integer getDrcStatus() {
        return drcStatus;
    }

    public void setDrcStatus(Integer drcStatus) {
        this.drcStatus = drcStatus;
    }

    public MhaQueryDto getRelatedMha() {
        return relatedMha;
    }

    public void setRelatedMha(MhaQueryDto relatedMha) {
        this.relatedMha = relatedMha;
    }

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
                ", relatedMha=" + relatedMha +
                "} " + super.toString();
    }
}
