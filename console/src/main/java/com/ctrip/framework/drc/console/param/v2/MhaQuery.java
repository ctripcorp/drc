package com.ctrip.framework.drc.console.param.v2;

import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Objects;

public class MhaQuery {
    private String containMhaName;
    private Long buId;
    private List<Long> dcIdList;

    public boolean emptyQueryCondition() {
        return StringUtils.isEmpty(containMhaName)
                && (buId == null || buId == 0)
                && CollectionUtils.isEmpty(dcIdList);
    }

    public String getContainMhaName() {
        return containMhaName;
    }

    public void setContainMhaName(String containMhaName) {
        this.containMhaName = containMhaName;
    }

    public Long getBuId() {
        return buId;
    }

    public void setBuId(Long buId) {
        this.buId = buId;
    }

    public List<Long> getDcIdList() {
        return dcIdList;
    }

    public void setDcIdList(List<Long> dcIdList) {
        this.dcIdList = dcIdList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MhaQuery)) return false;
        MhaQuery mhaQuery = (MhaQuery) o;
        return Objects.equals(containMhaName, mhaQuery.containMhaName) && Objects.equals(buId, mhaQuery.buId) && Objects.equals(dcIdList, mhaQuery.dcIdList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(containMhaName, buId, dcIdList);
    }
}
