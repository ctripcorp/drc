package com.ctrip.framework.drc.console.param.v2;

import com.ctrip.framework.drc.core.http.PageReq;

import java.util.List;
import java.util.Objects;

public class MhaReplicationQuery extends PageReq {
    private List<Long> srcMhaIdList;
    private List<Long> dstMhaIdList;
    private List<Long> relatedMhaIdList;
    private Integer drcStatus;

    public Integer getDrcStatus() {
        return drcStatus;
    }

    public void setDrcStatus(Integer drcStatus) {
        this.drcStatus = drcStatus;
    }

    public List<Long> getRelatedMhaIdList() {
        return relatedMhaIdList;
    }

    public void setRelatedMhaIdList(List<Long> relatedMhaIdList) {
        this.relatedMhaIdList = relatedMhaIdList;
    }

    public List<Long> getDstMhaIdList() {
        return dstMhaIdList;
    }

    public void setDstMhaIdList(List<Long> dstMhaIdList) {
        this.dstMhaIdList = dstMhaIdList;
    }

    public List<Long> getSrcMhaIdList() {
        return srcMhaIdList;
    }

    public void setSrcMhaIdList(List<Long> srcMhaIdList) {
        this.srcMhaIdList = srcMhaIdList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MhaReplicationQuery)) return false;
        if (!super.equals(o)) return false;
        MhaReplicationQuery query = (MhaReplicationQuery) o;
        return Objects.equals(srcMhaIdList, query.srcMhaIdList) && Objects.equals(dstMhaIdList, query.dstMhaIdList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), srcMhaIdList, dstMhaIdList);
    }
}
