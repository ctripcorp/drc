package com.ctrip.framework.drc.core.driver.binlog.gtid;


import java.util.List;
import java.util.Objects;

/**
 * @author yongnian
 */
public class Gtid {
    private final String uuid;
    private final long transactionId;

    public String getUuid() {
        return uuid;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public Gtid(String uuid, long transactionId) {
        this.uuid = uuid;
        this.transactionId = transactionId;
    }

    public Gtid(String gtid) {
        String[] split = gtid.split(":");
        if (split.length != 2) {
            throw new IllegalArgumentException("illegal gtid:" + gtid);
        }
        this.uuid = split[0];
        this.transactionId = Long.parseLong(split[1]);
    }


    public boolean isContainedWithin(GtidSet gtidSet) {
        if (gtidSet == null) {
            return false;
        }
        GtidSet.UUIDSet uuidSet = gtidSet.getUUIDSet(uuid);
        if (uuidSet == null) {
            return false;
        }
        List<GtidSet.Interval> intervals = uuidSet.getIntervals();
        if (intervals.isEmpty()) {
            return false;
        }
        int index = uuidSet.findInterval(transactionId);

        if (index >= intervals.size()) {
            return false;
        }
        GtidSet.Interval interval = intervals.get(index);
        return interval.getStart() <= transactionId && transactionId <= interval.getEnd();
    }

    @Override
    public String toString() {
        return uuid + ":" + transactionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Gtid)) return false;
        Gtid gtid = (Gtid) o;
        return transactionId == gtid.transactionId && Objects.equals(uuid, gtid.uuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid, transactionId);
    }
}

