package com.ctrip.framework.drc.console.vo.filter;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/5/8 10:50 下午
 */
public class RowsFilterMetaMappingVO {
    private Long metaFilterId;
    private List<String> filterKeys;

    @Override
    public String toString() {
        return "RowsFilterMetaMappingVO{" +
                "metaFilterId=" + metaFilterId +
                ", filterKeys=" + filterKeys +
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
}
