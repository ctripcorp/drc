package com.ctrip.framework.drc.core.monitor.entity;

import java.util.Objects;

/**
 * Created by jixinwang on 2022/9/7
 */
public class CostFlowKey {

    private String dbName;

    private String srcRegion;

    private String dstRegion;

    public CostFlowKey(String dbName, String srcRegion, String dstRegion) {
        this.dbName = dbName;
        this.srcRegion = srcRegion;
        this.dstRegion = dstRegion;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CostFlowKey that = (CostFlowKey) o;

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
        return "CostFlowKey{" +
                "dbName='" + dbName + '\'' +
                ", srcRegion='" + srcRegion + '\'' +
                ", dstRegion='" + dstRegion + '\'' +
                '}';
    }
}
