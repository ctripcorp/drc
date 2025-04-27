package com.ctrip.framework.drc.console.vo.api;

import org.springframework.util.CollectionUtils;

import java.util.Objects;

/**
 * @author yongnian
 * @create 2025/4/25 17:03
 */
public class SingleDbTableDrcCheckResponse {
    private final boolean hasDrc;
    private final DbTableDrcRegionInfo dbTableDrcRegionInfo;

    public SingleDbTableDrcCheckResponse(DbTableDrcRegionInfo dbTableDrcRegionInfo) {
        this.dbTableDrcRegionInfo = Objects.requireNonNull(dbTableDrcRegionInfo);
        this.hasDrc = !CollectionUtils.isEmpty(dbTableDrcRegionInfo.getRegions());
    }

    public boolean isHasDrc() {
        return hasDrc;
    }

    public DbTableDrcRegionInfo getDbTableDrcRegionInfo() {
        return dbTableDrcRegionInfo;
    }
}
