package com.ctrip.framework.drc.console.param.v2.resource;

import java.util.List;

/**
 * Created by dengquanliang
 * 2024/4/8 13:05
 */
public class ReplicatorMigrateParam {
    private String oldIp;
    private String newIp;
    private List<String> mhaList;

    public ReplicatorMigrateParam(String oldIp, String newIp, List<String> mhaList) {
        this.oldIp = oldIp;
        this.newIp = newIp;
        this.mhaList = mhaList;
    }

    public ReplicatorMigrateParam() {
    }

    public String getOldIp() {
        return oldIp;
    }

    public void setOldIp(String oldIp) {
        this.oldIp = oldIp;
    }

    public String getNewIp() {
        return newIp;
    }

    public void setNewIp(String newIp) {
        this.newIp = newIp;
    }

    public List<String> getMhaList() {
        return mhaList;
    }

    public void setMhaList(List<String> mhaList) {
        this.mhaList = mhaList;
    }
}
