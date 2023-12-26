package com.ctrip.framework.drc.console.vo.request;

import com.ctrip.framework.drc.core.http.PageReq;

import java.io.Serializable;


public class MhaDbReplicationQueryDto extends PageReq implements Serializable {
    private MhaDbQueryDto srcMhaDb;
    private MhaDbQueryDto dstMhaDb;
    private MhaDbQueryDto relatedMhaDb;

    private Integer drcStatus;

    public MhaDbQueryDto getSrcMhaDb() {
        return srcMhaDb;
    }

    public void setSrcMhaDb(MhaDbQueryDto srcMhaDb) {
        this.srcMhaDb = srcMhaDb;
    }

    public MhaDbQueryDto getDstMhaDb() {
        return dstMhaDb;
    }

    public void setDstMhaDb(MhaDbQueryDto dstMhaDb) {
        this.dstMhaDb = dstMhaDb;
    }

    public MhaDbQueryDto getRelatedMhaDb() {
        return relatedMhaDb;
    }

    public void setRelatedMhaDb(MhaDbQueryDto relatedMhaDb) {
        this.relatedMhaDb = relatedMhaDb;
    }

    public Integer getDrcStatus() {
        return drcStatus;
    }

    public void setDrcStatus(Integer drcStatus) {
        this.drcStatus = drcStatus;
    }
}
