package com.ctrip.framework.drc.console.vo.api;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class DbTableDrcRegionInfo {
    private final String db;
    private final String table;
    private final List<RegionInfo> regions;

    public DbTableDrcRegionInfo(String db, String table, List<RegionInfo> regions) {
        this.db = db;
        this.table = table;
        this.regions = Collections.unmodifiableList(regions);
    }

    public String getDb() {
        return db;
    }

    public String getTable() {
        return table;
    }

    public List<RegionInfo> getRegions() {
        return regions;
    }

    @Override
    public String toString() {
        return "DbTableDrcRegionInfo{" +
                "db='" + db + '\'' +
                ", table='" + table + '\'' +
                ", regions=" + regions +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DbTableDrcRegionInfo that)) return false;
        return Objects.equals(db, that.db) && Objects.equals(table, that.table) && Objects.equals(regions, that.regions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(db, table, regions);
    }
}
