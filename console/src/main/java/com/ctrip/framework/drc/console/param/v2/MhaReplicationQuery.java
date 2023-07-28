package com.ctrip.framework.drc.console.param.v2;

import com.ctrip.framework.drc.core.http.PageReq;

import java.util.List;

public class MhaReplicationQuery extends PageReq {
    private List<Long> srcMhaIdList;
    private List<Long> dstMhaIdList;

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
}
