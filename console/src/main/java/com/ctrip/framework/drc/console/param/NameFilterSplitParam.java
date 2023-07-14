package com.ctrip.framework.drc.console.param;

/**
 * Created by dengquanliang
 * 2023/6/14 17:30
 */
public class NameFilterSplitParam {
    private String nameFilter;
    private long applierGroupId;
    private String mhaName;

    public String getNameFilter() {
        return nameFilter;
    }

    public void setNameFilter(String nameFilter) {
        this.nameFilter = nameFilter;
    }

    public long getApplierGroupId() {
        return applierGroupId;
    }

    public void setApplierGroupId(long applierGroupId) {
        this.applierGroupId = applierGroupId;
    }

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    @Override
    public String toString() {
        return "NameFilterSplitParam{" +
                "nameFilter='" + nameFilter + '\'' +
                ", applierGroupId=" + applierGroupId +
                ", mhaName='" + mhaName + '\'' +
                '}';
    }
}
