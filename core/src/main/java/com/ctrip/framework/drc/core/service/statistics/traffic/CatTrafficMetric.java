package com.ctrip.framework.drc.core.service.statistics.traffic;

import java.util.Objects;

/**
 * Created by jixinwang on 2022/9/15
 */
public class CatTrafficMetric {

    private String buName;

    private String count;

    private Long size;

    private String dbName;

    public String getBuName() {
        return buName;
    }

    public void setBuName(String buName) {
        this.buName = buName;
    }

    public String getCount() {
        return count;
    }

    public void setCount(String count) {
        this.count = count;
    }

    public Long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CatTrafficMetric that = (CatTrafficMetric) o;

        if (!Objects.equals(buName, that.buName)) return false;
        if (!Objects.equals(count, that.count)) return false;
        if (!Objects.equals(size, that.size)) return false;
        return Objects.equals(dbName, that.dbName);
    }

    @Override
    public int hashCode() {
        int result = buName != null ? buName.hashCode() : 0;
        result = 31 * result + (count != null ? count.hashCode() : 0);
        result = 31 * result + (size != null ? size.hashCode() : 0);
        result = 31 * result + (dbName != null ? dbName.hashCode() : 0);
        return result;
    }
}
