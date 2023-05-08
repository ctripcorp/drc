package com.ctrip.framework.drc.console.vo.filter;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/4/26 16:20
 */
public class RowsFilterMetaMessageVO {

    private Long metaFilterId;
    private String metaFilterName;
    private String bu;
    private String owner;
    private List<String> targetSubEnv;
    private Integer filterType;
    private String token;

    @Override
    public String toString() {
        return "RowsFilterMetaMessageVO{" +
                "metaFilterId=" + metaFilterId +
                ", metaFilterName='" + metaFilterName + '\'' +
                ", bu='" + bu + '\'' +
                ", owner='" + owner + '\'' +
                ", targetSubEnv=" + targetSubEnv +
                ", filterType=" + filterType +
                ", token='" + token + '\'' +
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

    public List<String> getTargetSubEnv() {
        return targetSubEnv;
    }

    public void setTargetSubEnv(List<String> targetSubEnv) {
        this.targetSubEnv = targetSubEnv;
    }

    public Integer getFilterType() {
        return filterType;
    }

    public void setFilterType(Integer filterType) {
        this.filterType = filterType;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

}
