package com.ctrip.framework.drc.core.monitor.entity;

import java.util.Objects;

/**
 * Created by jixinwang on 2022/9/7
 */
public class TrafficStatisticKey {

    private String dbName;

    private String srcRegion;

    private String dstRegion;

    private String dstType;

    public TrafficStatisticKey(String dbName, String srcRegion, String dstRegion, String dstType) {
        this.dbName = dbName;
        this.srcRegion = srcRegion;
        this.dstRegion = dstRegion;
        this.dstType = dstType;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getSrcRegion() {
        return srcRegion;
    }

    public void setSrcRegion(String srcRegion) {
        this.srcRegion = srcRegion;
    }

    public String getDstRegion() {
        return dstRegion;
    }

    public void setDstRegion(String dstRegion) {
        this.dstRegion = dstRegion;
    }

    public String getDstType() {
        return dstType;
    }

    public void setDstType(String dstType) {
        this.dstType = dstType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TrafficStatisticKey that = (TrafficStatisticKey) o;

        if (!Objects.equals(dbName, that.dbName)) return false;
        if (!Objects.equals(srcRegion, that.srcRegion)) return false;
        return Objects.equals(dstRegion, that.dstRegion);
    }

    @Override
    public int hashCode() {
        int result = dbName != null ? dbName.hashCode() : 0;
        result = 31 * result + (srcRegion != null ? srcRegion.hashCode() : 0);
        result = 31 * result + (dstRegion != null ? dstRegion.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TrafficStatisticKey{" +
                "dbName='" + dbName + '\'' +
                ", srcRegion='" + srcRegion + '\'' +
                ", dstRegion='" + dstRegion + '\'' +
                ", dstType='" + dstType + '\'' +
                '}';
    }
}
