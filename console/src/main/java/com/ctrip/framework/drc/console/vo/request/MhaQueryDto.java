package com.ctrip.framework.drc.console.vo.request;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

public class MhaQueryDto implements Serializable {

    private String name;

    private Long buId;

    private Long regionId;
    // split with ","
    private String dbNames;
    public String getDbNames() {
        return dbNames;
    }

    public void setDbNames(String dbNames) {
        this.dbNames = dbNames;
    }

    public boolean isConditionalQuery() {
        return StringUtils.isNotBlank(name)
                || (buId != null && buId > 0)
                || (regionId != null && regionId > 0);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getBuId() {
        return buId;
    }

    public void setBuId(Long buId) {
        this.buId = buId;
    }

    public Long getRegionId() {
        return regionId;
    }


    public void setRegionId(Long regionId) {
        this.regionId = regionId;
    }

    @Override
    public String toString() {
        return "MhaQueryDto{" +
                "name='" + name + '\'' +
                ", buId=" + buId +
                ", regionId=" + regionId +
                '}';
    }
}
