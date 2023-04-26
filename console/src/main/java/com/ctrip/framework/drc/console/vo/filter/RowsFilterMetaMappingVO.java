package com.ctrip.framework.drc.console.vo.filter;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/4/26 16:20
 */
public class RowsFilterMetaMappingVO {

    private Long metaFilterId;
    private String metaFilterName;
    private String bu;
    private String owner;
    private List<String> targetSubenv;
    private String filterType;
    private String token;
    private List<String> filterKeys;

    @Override
    public String toString() {
        return "RowsFilterMetaMappingVO{" +
                "metaFilterId=" + metaFilterId +
                ", metaFilterName='" + metaFilterName + '\'' +
                ", bu='" + bu + '\'' +
                ", owner='" + owner + '\'' +
                ", targetSubenv=" + targetSubenv +
                ", filterType='" + filterType + '\'' +
                ", token='" + token + '\'' +
                ", filterKeys=" + filterKeys +
                '}';
    }

    public Long getMetaFilterId() {
        return metaFilterId;
    }

    public void setMetaFilterId(Long metaFilterId) {
        this.metaFilterId = metaFilterId;
    }

    public String getMetaFilterName() {
        return metaFilterName;
    }

    public void setMetaFilterName(String metaFilterName) {
        this.metaFilterName = metaFilterName;
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

    public List<String> getTargetSubenv() {
        return targetSubenv;
    }

    public void setTargetSubenv(List<String> targetSubenv) {
        this.targetSubenv = targetSubenv;
    }

    public String getFilterType() {
        return filterType;
    }

    public void setFilterType(String filterType) {
        this.filterType = filterType;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public List<String> getFilterKeys() {
        return filterKeys;
    }

    public void setFilterKeys(List<String> filterKeys) {
        this.filterKeys = filterKeys;
    }
}
