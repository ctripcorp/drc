package com.ctrip.framework.drc.core.meta;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @Author Slight
 * Nov 07, 2019
 */
public class ZoneInfo {

    public int master;
    public List<DBInfo> dbs;

    public int getMaster() {
        return master;
    }

    public void setMaster(int master) {
        this.master = master;
    }

    public List<DBInfo> getDbs() {
        return dbs;
    }

    public void setDbs(List<DBInfo> dbs) {
        this.dbs = dbs;
    }

    public DBInfo retrieveMasterDBInfo() {
        return dbs.get(master);
    }

    public ReplicatorConcern asReplicatorConcerned() {

        ReplicatorConcern zone = new ReplicatorConcern();

        zone.master = dbs.get(master);
        zone.uuids = dbs.stream().map(
                DBInfo -> DBInfo.uuid
        ).collect(Collectors.toList());

        return zone;
    }

    public static class ReplicatorConcern {
        public DBInfo master;
        public List<String> uuids;

        public DBInfo getMaster() {
            return master;
        }

        public void setMaster(DBInfo master) {
            this.master = master;
        }

        public List<String> getUuids() {
            return uuids;
        }

        public void setUuids(List<String> uuids) {
            this.uuids = uuids;
        }

        @Override
        public String toString() {
            return "ReplicatorConcern{" +
                    "master=" + master +
                    ", uuids=" + uuids +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ReplicatorConcern)) return false;
            ReplicatorConcern that = (ReplicatorConcern) o;
            return Objects.equals(master, that.master) &&
                    Objects.equals(uuids, that.uuids);
        }

        @Override
        public int hashCode() {
            return Objects.hash(master, uuids);
        }
    }

    @Override
    public String toString() {
        return "ZoneInfo{" +
                "master=" + master +
                ", dbs=" + dbs +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ZoneInfo)) return false;
        ZoneInfo zoneInfo = (ZoneInfo) o;
        return master == zoneInfo.master &&
                Objects.equals(dbs, zoneInfo.dbs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(master, dbs);
    }
}
