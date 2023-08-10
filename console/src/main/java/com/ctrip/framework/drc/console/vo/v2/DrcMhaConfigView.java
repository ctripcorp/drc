package com.ctrip.framework.drc.console.vo.v2;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/8/1 18:18
 */
public class DrcMhaConfigView {
    private String mhaName;
    private List<String> replicatorIps;
    private List<String> applierIps;
    private String applierInitGtid;

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public List<String> getReplicatorIps() {
        return replicatorIps;
    }

    public void setReplicatorIps(List<String> replicatorIps) {
        this.replicatorIps = replicatorIps;
    }

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
