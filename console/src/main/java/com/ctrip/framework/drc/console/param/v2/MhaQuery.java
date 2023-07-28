package com.ctrip.framework.drc.console.param.v2;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

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
}
