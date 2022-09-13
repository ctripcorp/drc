package com.ctrip.framework.drc.core.service.statistics.traffic;

/**
 * Created by jixinwang on 2022/9/9
 */
public class HickWallTrafficMetric {

    private String dbName;

    private String srcRegion;

    private String dstRegion;

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

        HickWallTrafficMetric metric = (HickWallTrafficMetric) o;

        if (!dbName.equals(metric.dbName)) return false;
        if (!srcRegion.equals(metric.srcRegion)) return false;
        return dstRegion.equals(metric.dstRegion);
    }

    @Override
    public int hashCode() {
        int result = dbName.hashCode();
        result = 31 * result + srcRegion.hashCode();
        result = 31 * result + dstRegion.hashCode();
        return result;
    }
}
