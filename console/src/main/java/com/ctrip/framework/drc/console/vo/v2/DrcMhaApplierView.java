package com.ctrip.framework.drc.console.vo.v2;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/8/9 20:20
 */
public class DrcMhaApplierView {
    private List<String> applierIps;
    private String applierInitGtid;

    public List<String> getApplierIps() {
        return applierIps;
    }

    public void setApplierIps(List<String> applierIps) {
        this.applierIps = applierIps;
    }

    public String getApplierInitGtid() {
        return applierInitGtid;
    }

    public void setApplierInitGtid(String applierInitGtid) {
        this.applierInitGtid = applierInitGtid;
    }
}
