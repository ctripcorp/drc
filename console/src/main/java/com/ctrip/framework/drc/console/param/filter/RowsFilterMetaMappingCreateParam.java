package com.ctrip.framework.drc.console.param.filter;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/4/26 14:43
 */
public class RowsFilterMetaMappingCreateParam {

    private Long metaFilterId;
    private List<String> filterKeys;

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

    @Override
    public String toString() {
        return "RowsFilterMetaMappingCreateParam{" +
                "metaFilterId=" + metaFilterId +
                ", filterKeys=" + filterKeys +
                '}';
    }
}
