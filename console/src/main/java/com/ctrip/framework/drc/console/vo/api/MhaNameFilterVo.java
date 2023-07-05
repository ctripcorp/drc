package com.ctrip.framework.drc.console.vo.api;

import java.util.Set;

/**
 * Created by dengquanliang
 * 2023/6/7 15:27
 */
public class MhaNameFilterVo {
    private String mhaName;
    private long applierGroupId;
    private String nameFilter;
    private Set<String> filterTables;

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public Set<String> getFilterTables() {
        return filterTables;
    }

    public void setFilterTables(Set<String> filterTables) {
        this.filterTables = filterTables;
    }

    public long getApplierGroupId() {
        return applierGroupId;
    }

    public void setApplierGroupId(long applierGroupId) {
        this.applierGroupId = applierGroupId;
    }

    public String getNameFilter() {
        return nameFilter;
    }

    public void setNameFilter(String nameFilter) {
        this.nameFilter = nameFilter;
    }

    @Override
    public String toString() {
        return "MhaNameFilterVo{" +
                "mhaName='" + mhaName + '\'' +
                ", applierGroupId=" + applierGroupId +
                ", nameFilter='" + nameFilter + '\'' +
                ", filterTables=" + filterTables +
                '}';
    }
}
