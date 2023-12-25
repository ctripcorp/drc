package com.ctrip.framework.drc.console.vo.request;

import com.ctrip.framework.drc.console.utils.NumberUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

public class MhaDbQueryDto implements Serializable {


    /**
     * mha related
     */
    private Long regionId;
    /**
     * db related
     */
    private String dbName;
    private String buCode;

    public boolean isConditionalQuery() {
        return hasDbCondition() || hasMhaCondition();
    }

    public boolean hasDbCondition() {
        return StringUtils.isNotBlank(dbName) || StringUtils.isNotBlank(buCode);
    }

    public boolean hasMhaCondition() {
        return NumberUtils.isPositive(regionId);
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getBuCode() {
        return buCode;
    }

    public void setBuCode(String buCode) {
        this.buCode = buCode;
    }

    public Long getRegionId() {
        return regionId;
    }


    public void setRegionId(Long regionId) {
        this.regionId = regionId;
    }

    @Override
    public String toString() {
        return "MhaDbQueryDto{" +
                "dbName='" + dbName + '\'' +
                ", buId=" + buCode +
                ", regionId=" + regionId +
                '}';
    }
}
