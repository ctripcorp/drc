package com.ctrip.framework.drc.console.param.v2;

import com.ctrip.framework.drc.core.http.PageReq;

import java.util.List;

public class MhaReplicationQuery extends PageReq {
    private List<Long> srcMhaIdList;
    private List<Long> desMhaIdList;

    public List<Long> getDesMhaIdList() {
        return desMhaIdList;
    }

    public void setDesMhaIdList(List<Long> desMhaIdList) {
        this.desMhaIdList = desMhaIdList;
    }

    public List<Long> getSrcMhaIdList() {
        return srcMhaIdList;
    }

    public void setSrcMhaIdList(List<Long> srcMhaIdList) {
        this.srcMhaIdList = srcMhaIdList;
    }
}
