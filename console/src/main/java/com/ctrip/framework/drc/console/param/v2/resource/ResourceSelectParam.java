package com.ctrip.framework.drc.console.param.v2.resource;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/8/8 15:13
 */
public class ResourceSelectParam {
    private String mhaName;
    private int type;
    private List<String> selectedIps;

    public ResourceSelectParam() {
    }

    public ResourceSelectParam(String mhaName, int type, List<String> selectedIps) {
        this.mhaName = mhaName;
        this.type = type;
        this.selectedIps = selectedIps;
    }

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public List<String> getSelectedIps() {
        return selectedIps;
    }

    public void setSelectedIps(List<String> selectedIps) {
        this.selectedIps = selectedIps;
    }
}
