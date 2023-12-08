package com.ctrip.framework.drc.console.param.v2;

import com.ctrip.framework.drc.core.http.PageReq;

import java.util.List;
import java.util.Objects;

public class MhaDbReplicationQuery extends PageReq {
    private List<Long> srcMappingIdList;
    private List<Long> dstMappingIdList;
    private List<Long> relatedMappingList;
    private Integer type;

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public List<Long> getRelatedMappingList() {
        return relatedMappingList;
    }

    public void setRelatedMappingList(List<Long> relatedMappingList) {
        this.relatedMappingList = relatedMappingList;
    }

    public List<Long> getDstMappingIdList() {
        return dstMappingIdList;
    }

    public void setDstMappingIdList(List<Long> dstMappingIdList) {
        this.dstMappingIdList = dstMappingIdList;
    }

    public List<Long> getSrcMappingIdList() {
        return srcMappingIdList;
    }

    public void setSrcMappingIdList(List<Long> srcMappingIdList) {
        this.srcMappingIdList = srcMappingIdList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MhaDbReplicationQuery)) return false;
        if (!super.equals(o)) return false;
        MhaDbReplicationQuery that = (MhaDbReplicationQuery) o;
        return Objects.equals(srcMappingIdList, that.srcMappingIdList) && Objects.equals(dstMappingIdList, that.dstMappingIdList) && Objects.equals(relatedMappingList, that.relatedMappingList) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), srcMappingIdList, dstMappingIdList, relatedMappingList, type);
    }

    @Override
    public String toString() {
        return "MhaDbReplicationQuery{" +
                "srcMappingIdList=" + srcMappingIdList +
                ", dstMappingIdList=" + dstMappingIdList +
                ", relatedMappingList=" + relatedMappingList +
                ", type=" + type +
                "} " + super.toString();
    }
}
