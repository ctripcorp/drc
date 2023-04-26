package com.ctrip.framework.drc.console.param.filter;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/4/25 17:37
 */
public class RowsFilterMetaMessageCreateParam {
    private String clusterName;
    private String srcMha;
    private String desMha;
    private List<String> targetSubenv;
    private String bu;
    private String owner;
    //1-blacklist 2-whitelist
    private Integer filterType;

    @Override
    public String toString() {
        return "RowsFilterMetaMessageCreateParam{" +
                "clusterName='" + clusterName + '\'' +
                ", srcMha='" + srcMha + '\'' +
                ", desMha='" + desMha + '\'' +
                ", targetSubenv='" + targetSubenv + '\'' +
                ", bu='" + bu + '\'' +
                ", owner='" + owner + '\'' +
                ", filterType=" + filterType +
                '}';
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getSrcMha() {
        return srcMha;
    }

    public void setSrcMha(String srcMha) {
        this.srcMha = srcMha;
    }

    public String getDesMha() {
        return desMha;
    }

    public void setDesMha(String desMha) {
        this.desMha = desMha;
    }

    public List<String> getTargetSubenv() {
        return targetSubenv;
    }

    public void setTargetSubenv(List<String> targetSubenv) {
        this.targetSubenv = targetSubenv;
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
