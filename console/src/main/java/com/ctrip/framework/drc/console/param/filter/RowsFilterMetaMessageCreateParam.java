package com.ctrip.framework.drc.console.param.filter;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/4/25 17:37
 */
public class RowsFilterMetaMessageCreateParam {
    private String metaFilterName;
    private List<String> targetSubEnv;
    private String bu;
    private String owner;
    //1-blacklist 2-whitelist
    private Integer filterType;

    @Override
    public String toString() {
        return "RowsFilterMetaMessageCreateParam{" +
                "metaFilterName='" + metaFilterName + '\'' +
                ", targetSubEnv=" + targetSubEnv +
                ", bu='" + bu + '\'' +
                ", owner='" + owner + '\'' +
                ", filterType=" + filterType +
                '}';
    }

    public String getMetaFilterName() {
        return metaFilterName;
    }

    public void setMetaFilterName(String metaFilterName) {
        this.metaFilterName = metaFilterName;
    }

    public List<String> getTargetSubEnv() {
        return targetSubEnv;
    }

    public void setTargetSubEnv(List<String> targetSubEnv) {
        this.targetSubEnv = targetSubEnv;
    }

    public String getBu() {
        return bu;
    }

    public void setBu(String bu) {
        this.bu = bu;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public Integer getFilterType() {
        return filterType;
    }

    public void setFilterType(Integer filterType) {
        this.filterType = filterType;
    }
}
