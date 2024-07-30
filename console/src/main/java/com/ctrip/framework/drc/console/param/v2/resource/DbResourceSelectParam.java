package com.ctrip.framework.drc.console.param.v2.resource;

import java.util.List;


public class DbResourceSelectParam {
    private String srcMhaName;
    private String dstMhaName;
    private int type;
    private List<String> selectedIps;

    public DbResourceSelectParam() {
    }

    public DbResourceSelectParam(String srcMhaName, String dstMhaName, int type, List<String> selectedIps) {
        this.srcMhaName = srcMhaName;
        this.dstMhaName = dstMhaName;
        this.type = type;
        this.selectedIps = selectedIps;
    }

    public String getDstMhaName() {
        return dstMhaName;
    }

    public void setDstMhaName(String dstMhaName) {
        this.dstMhaName = dstMhaName;
    }

    public String getSrcMhaName() {
        return srcMhaName;
    }

    public void setSrcMhaName(String srcMhaName) {
        this.srcMhaName = srcMhaName;
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

    @Override
    public String toString() {
        return "DbResourceSelectParam{" +
                "srcMhaName='" + srcMhaName + '\'' +
                ", dstMhaName='" + dstMhaName + '\'' +
                ", type=" + type +
                ", selectedIps=" + selectedIps +
                '}';
    }
}
