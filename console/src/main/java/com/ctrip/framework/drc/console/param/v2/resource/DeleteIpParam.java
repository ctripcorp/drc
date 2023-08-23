package com.ctrip.framework.drc.console.param.v2.resource;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/8/17 11:33
 */
public class DeleteIpParam {
    private List<String> unusedIps;
    private List<String> existIps;

    public List<String> getUnusedIps() {
        return unusedIps;
    }

    public void setUnusedIps(List<String> unusedIps) {
        this.unusedIps = unusedIps;
    }

    public List<String> getExistIps() {
        return existIps;
    }

    public void setExistIps(List<String> existIps) {
        this.existIps = existIps;
    }
}
