package com.ctrip.framework.drc.console.vo.filter;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/5/8 10:50 下午
 */
public class RowsFilterMetaMappingVO {
    private Long metaFilterId;
    private List<String> filterKeys;
    private String filterValue;
    private String token;
    private Integer filterType;

    @Override
    public String toString() {
        return "RowsFilterMetaMappingVO{" +
                "metaFilterId=" + metaFilterId +
                ", filterKeys=" + filterKeys +
                ", filterValue='" + filterValue + '\'' +
                ", token='" + token + '\'' +
                ", filterType=" + filterType +
                '}';
    }

    public Long getMetaFilterId() {
        return metaFilterId;
    }

    public void setMetaFilterId(Long metaFilterId) {
        this.metaFilterId = metaFilterId;
    }

    public List<String> getFilterKeys() {
        return filterKeys;
    }

    public void setFilterKeys(List<String> filterKeys) {
        this.filterKeys = filterKeys;
    }

    public String getFilterValue() {
        return filterValue;
    }

    public void setFilterValue(String filterValue) {
        this.filterValue = filterValue;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public Integer getFilterType() {
        return filterType;
    }

    public void setFilterType(Integer filterType) {
        this.filterType = filterType;
    }
}
