package com.ctrip.framework.drc.core.driver.binlog.gtid;

import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by mingdongli
 * 2019/9/25 下午10:10.
 */
public class GtidSet {

    private final Map<String, UUIDSet> map = new LinkedHashMap<>();

    public GtidSet(Map<String, UUIDSet> uuidSets) {
        if (null != uuidSets && !uuidSets.isEmpty()) {
            uuidSets.forEach(map::put);
        }
    }

    /**
     * @param gtidSet gtid set comprised of closed intervals (like MySQL's executed_gtid_set).
     */
    public GtidSet(String gtidSet) {
        String[] uuidSets = (gtidSet == null || gtidSet.isEmpty()) ? new String[0] :
                gtidSet.replace("\n", "").split(",");
        for (String uuidSet : uuidSets) {
            int uuidSeparatorIndex = uuidSet.indexOf(":");
            String sourceId = uuidSet.substring(0, uuidSeparatorIndex);
            List<Interval> intervals = new ArrayList<>();
            String[] rawIntervals = uuidSet.substring(uuidSeparatorIndex + 1).split(":");
            for (String interval : rawIntervals) {
                String[] is = interval.split("-");
                long[] split = new long[is.length];
                for (int i = 0, e = is.length; i < e; i++) {
                    split[i] = Long.parseLong(is[i]);
                }
                if (split.length == 1) {
                    split = new long[] {split[0], split[0]};
                }
                intervals.add(new Interval(split[0], split[1]));
            }
            map.put(sourceId, new UUIDSet(sourceId, intervals));
        }
    }

    public byte[] encode() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteHelper.writeUnsignedInt64LittleEndian(map.size(), out);

        for (UUIDSet uuidSet : map.values()) {
            out.write(uuidSet.encode());
        }

        return out.toByteArray();
    }

    public void decode(byte[] data) {
        int index = 0;
        long size = ByteHelper.readUnsignedLongLittleEndian(data, index);
        index += 8;

        for (int i = 0; i < size; i++) {
            UUIDSet uuidSet = new UUIDSet();
            int readSize = uuidSet.decode(data, index);
            map.put(uuidSet.uuid, uuidSet);
            index = readSize;
        }

    }

    /**
     * Get an immutable collection of the {@link UUIDSet range of GTIDs for a single server}.
     * @return the {@link UUIDSet GTID ranges for each server}; never null
     */
    public Collection<UUIDSet> getUUIDSets() {
        return Collections.unmodifiableCollection(map.values());
    }

    public Set<String> getUUIDs() {
        return Collections.unmodifiableSet(map.keySet());
    }

    /**
     * Find the {@link UUIDSet} for the server with the specified UUID.
     * @param uuid the UUID of the server
     * @return the {@link UUIDSet} for the identified server, or {@code null} if there are no GTIDs from that server.
     */
    public UUIDSet getUUIDSet(String uuid) {
        return map.get(uuid);
    }

    /**
     * Add or replace the UUIDSet
     * @param uuidSet UUIDSet to be added
     * @return the old {@link UUIDSet} for the server given in uuidSet param,
     *         or {@code null} if there are no UUIDSet for the given server.
     */
    public UUIDSet putUUIDSet(UUIDSet uuidSet) {
        return map.put(uuidSet.getUUID(), uuidSet);
    }

    /**
     * @param gtid GTID ("source_id:transaction_id")
     * @return whether or not gtid was added to the set (false if it was already there)
     */
    public boolean add(String gtid) {
        String[] split = gtid.split(":");
        if (split.length != 2) {
            return false;
        }
        String sourceId = split[0];
        long transactionId = Long.parseLong(split[1]);
        UUIDSet uuidSet = map.get(sourceId);
        if (uuidSet == null) {
            map.put(sourceId, uuidSet = new UUIDSet(sourceId, new ArrayList<Interval>()));
        }
        return uuidSet.add(transactionId);
    }

    public boolean addAndFillGap(String gtid) {
        String[] split = gtid.split(":");
        if (split.length != 2) {
            return false;
        }
        String sourceId = split[0];
        long transactionId = Long.parseLong(split[1]);
        return addAndFillGap(sourceId, transactionId);
    }

    public boolean addAndFillGap(String sourceId, long transactionId) {
        UUIDSet uuidSet = map.get(sourceId);
        if (uuidSet == null) {
            map.put(sourceId, uuidSet = new UUIDSet(sourceId, new ArrayList<>()));
        }
        return uuidSet.addAndFillGap(transactionId);
    }

    public GtidSet subtract(GtidSet other) {
        if (other == null || other.map == null || other.map.size() == 0) {
            return clone();
        }
        GtidSet clone = clone();
        for (Map.Entry<String, UUIDSet> entry : other.map.entrySet()) {
            String uuid = entry.getKey();
            UUIDSet uuidSet = clone.getUUIDSet(uuid);
            if (uuidSet == null) {
                continue;
            }
            UUIDSet otherUUIDSet = entry.getValue();
            for (Interval interval : otherUUIDSet.getIntervals()) {
                removeInterval(uuidSet, interval);
                if (uuidSet.intervals.isEmpty()) {
                    clone.map.remove(uuid);
                }
            }
        }

        return clone;
    }

    public GtidSet union(GtidSet other) {
        if (other == null || other.map.size() == 0) {
            return clone();
        }
        GtidSet clone = clone();
        GtidSet otherClone = other.clone();
        for (Map.Entry<String, UUIDSet> entry : otherClone.map.entrySet()) {
            String uuid = entry.getKey();
            UUIDSet uuidSet = clone.getUUIDSet(uuid);
            if (uuidSet == null) {
                clone.putUUIDSet(entry.getValue());
                continue;
            }
            UUIDSet otherUUIDSet = entry.getValue();
            for (Interval interval : otherUUIDSet.getIntervals()) {
                addInterval(uuidSet, interval);
            }
        }

        return new GtidSet(clone.toString());  // transfer UUID:1-10:11-20 to UUID:1-20
    }

    public void unionInPlace(GtidSet other) {
        if (other == null || other.map.size() == 0) {
            return;
        }
        GtidSet otherClone = other.clone();
        for (Map.Entry<String, UUIDSet> entry : otherClone.map.entrySet()) {
            String uuid = entry.getKey();
            UUIDSet uuidSet = this.getUUIDSet(uuid);
            if (uuidSet == null) {
                this.putUUIDSet(entry.getValue());
                continue;
            }
            UUIDSet otherUUIDSet = entry.getValue();
            for (Interval interval : otherUUIDSet.getIntervals()) {
                addInterval(uuidSet, interval);
            }
        }
    }

    private void removeInterval(UUIDSet uuidSet, Interval other) {
        List<Interval> intervals = uuidSet.getMutableIntervals();
        if (intervals == null || intervals.isEmpty()) {
            return;
        }

        int index = -1;
        for (int i = 0; i < intervals.size(); ++i) {
            if (intervals.get(i).end >= other.start) {
                index = i;
                break;
            }
        }

        if (index < 0) {
            return;
        }

        Interval interval = intervals.get(index);
        if (interval.start < other.start) {
            if (interval.end > other.end) {
                Interval newInterval = new Interval(other.end + 1, interval.end);
                interval.end = other.start - 1;
                index++;
                intervals.add(index, newInterval);
                return;
            }

            interval.end = other.start - 1;
            index++;
            if (index == intervals.size()) {
                return;
            }
        }

        while (intervals.get(index).end <= other.end) {
            intervals.remove(index);
            if (index == intervals.size()) {
                return;
            }
        }

        if (intervals.get(index).start < other.end) {
            intervals.get(index).start = other.end + 1;
        }
    }

    private void addInterval(UUIDSet uuidSet, Interval other) {
        List<Interval> intervals = uuidSet.getMutableIntervals();
        int index = 0;

        if (intervals != null && !intervals.isEmpty()) {
            for (; index < intervals.size(); ++index) {
                if (intervals.get(index).end >= other.start) {
                    if (intervals.get(index).start > other.end) {
                        break;
                    }
                    if (intervals.get(index).start < other.start) {
                        other.start = intervals.get(index).start;
                    }
                    while ((index < intervals.size() - 1) && other.end >= intervals.get(index + 1).start) {
                        intervals.remove(index);
                    }

                    intervals.get(index).start = other.start;
                    if (intervals.get(index).end < other.end) {
                        intervals.get(index).end = other.end;
                    }
                    uuidSet.joinAdjacentIntervals(index);
                    return;
                }
            }
        }

        Interval newInterval = new Interval(other.start, other.end);
        if (intervals == null) {
            intervals = Lists.newArrayList();
        }
        intervals.add(index, newInterval);
        uuidSet.joinAdjacentIntervals(index);
    }

    public GtidSet replaceGtid(GtidSet otherGtidSet, String replaceUuid) {
        if (StringUtils.isBlank(replaceUuid)) {
            return clone();
        }

        Map<String, GtidSet.UUIDSet> uuidSets = Maps.newHashMap();

        for (String uuid : getUUIDs()) {
            GtidSet.UUIDSet uuidSet;
            if (uuid.equalsIgnoreCase(replaceUuid)) {
                uuidSet = otherGtidSet.getUUIDSet(uuid);
            } else {
                uuidSet = getUUIDSet(uuid);
            }
            if (uuidSet != null) {
                uuidSets.put(uuid, uuidSet);
            }
        }
        return new GtidSet(uuidSets);
    }

    public GtidSet filterGtid(Set<String> filteredUuid) {
        if (filteredUuid == null || filteredUuid.isEmpty()) {
            return new GtidSet("");
        }

        Map<String, GtidSet.UUIDSet> uuidSets = Maps.newHashMap();
        for (String uuid : filteredUuid) {
            GtidSet.UUIDSet uuidSet = this.getUUIDSet(uuid);
            if (uuidSet != null) {
                uuidSets.put(uuid, uuidSet);
            }
        }
        return new GtidSet(uuidSets);
    }

    /**
     * @return intersection (uuid) of two set
     */
    public GtidSet getIntersectionUUIDs(GtidSet otherGtidSet) {
        Set<String> currentUuids = getUUIDs();
        Set<String> executedUuids = otherGtidSet.getUUIDs();
        Set<String> intersectionUuids = new HashSet<>(currentUuids);
        intersectionUuids.retainAll(executedUuids);
        return filterGtid(intersectionUuids);
    }

    /**
     * @return intersection (uuid && intervals) of two set
     */
    public GtidSet getIntersection(GtidSet other) {
        Map<String, GtidSet.UUIDSet> uuidSets = Maps.newHashMap();
        Set<String> uuids = getUUIDs();
        for (String uuid : uuids) {
            UUIDSet currSet = this.getUUIDSet(uuid);
            UUIDSet otherSet = other.getUUIDSet(uuid);
            if (currSet != null && otherSet != null) {
                uuidSets.put(uuid, currSet.getIntersection(otherSet));
            }
        }
        return new GtidSet(uuidSets);
    }


    /**
     * @return get intersections of all
     */
    public static GtidSet getIntersection(List<GtidSet> list) {
        list = list.stream().filter(e -> !CollectionUtils.isEmpty(e.getUUIDs())).collect(Collectors.toList());
        GtidSet res = new GtidSet("");
        if (CollectionUtils.isEmpty(list)) {
            return res;
        }
        res = list.get(0).clone();
        for (int i = 1; i < list.size(); i++) {
            res = res.getIntersection(list.get(i));
        }
        return res;
    }


    public GtidSet getGtidFirstInterval() {
        GtidSet res = new GtidSet("");
        Set<String> uuids = getUUIDs();
        for (String uuid : uuids) {
            GtidSet.UUIDSet uuidSet = getUUIDSet(uuid);
            if (uuidSet == null || CollectionUtils.isEmpty(uuidSet.getIntervals())) {
                continue;
            }
            // only put 1st interval
            res.putUUIDSet(new GtidSet.UUIDSet(uuid, Lists.newArrayList(uuidSet.getIntervals().get(0))));
        }
        return res;
    }
    
    public GtidSet findFirstGap() {
        GtidSet res = new GtidSet("");
        Set<String> uuids = this.getUUIDs();
        for (String uuid : uuids) {
            GtidSet.UUIDSet uuidSet = this.getUUIDSet(uuid);
            if (uuidSet == null || CollectionUtils.isEmpty(uuidSet.getIntervals())) {
                continue;
            }
            List<Interval> intervals = uuidSet.getIntervals();
            if (intervals.get(0).start > 1) {
                res.putUUIDSet(new GtidSet.UUIDSet(uuid,Lists.newArrayList(new Interval(1, intervals.get(0).start - 1))));
                continue;
            }
            if (intervals.size() > 1) {
                res.putUUIDSet(new GtidSet.UUIDSet(uuid,Lists.newArrayList(new Interval(intervals.get(0).end + 1, intervals.get(1).start - 1))));
            }
        }
        return res;
    }

    public boolean isContainedWithin(String gtid) {
        if (StringUtils.isBlank(gtid)) {
            return false;
        }
        String[] uuidAndGno = gtid.split(":");
        UUIDSet uuidSet = getUUIDSet(uuidAndGno[0]);
        if (uuidSet == null) {
            return false;
        }
        List<Interval> intervals = uuidSet.getIntervals();
        if (intervals == null || intervals.isEmpty()) {
            return false;
        }
        Interval interval = intervals.get(intervals.size() - 1);
        return Long.parseLong(uuidAndGno[1]) <= interval.getEnd();
    }

    public GtidSet expandTo(String gtid) {
        if (StringUtils.isBlank(gtid)) {
            return this;
        }
        String[] uuidAndGno = gtid.split(":");
        UUIDSet uuidSet = getUUIDSet(uuidAndGno[0]);
        if (uuidSet == null) {
            add(gtid);
            return this;
        }
        List<Interval> intervals = uuidSet.getIntervals();
        if (intervals == null || intervals.isEmpty()) {
            add(gtid);
            return this;
        }
        Interval interval = intervals.get(intervals.size() - 1);
        interval.start =  Long.parseLong(uuidAndGno[1]);
        interval.end =  Long.parseLong(uuidAndGno[1]);
        return this;
    }

    /**
     * Determine if the GTIDs represented by this object are contained completely within the supplied set of GTIDs.
     * Note that if two {@link GtidSet}s are equal, then they both are subsets of the other.
     * @param other the other set of GTIDs; may be null
     * @return {@code true} if all of the GTIDs in this set are equal to or completely contained within the supplied
     * set of GTIDs, or {@code false} otherwise
     */
    public boolean isContainedWithin(GtidSet other) {
        if (other == null) {
            return false;
        }
        if (this == other) {
            return true;
        }
        if (this.equals(other)) {
            return true;
        }
        for (UUIDSet uuidSet : map.values()) {
            UUIDSet thatSet = other.getUUIDSet(uuidSet.getUUID());
            if (!uuidSet.isContainedWithin(thatSet)) {
                return false;
            }
        }
        return true;
    }

    public long getGtidNum() {
        int count = 0;
        for (UUIDSet value : map.values()) {
            List<Interval> intervals = value.getIntervals();
            for (Interval interval : intervals) {
                count += interval.getEnd() - interval.getStart() + 1;
            }
        }
        return count;
    }

    public GtidSet clone() {
        return new GtidSet(this.toString());
    }

    @Override
    public int hashCode() {
        return map.keySet().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof GtidSet) {
            GtidSet that = (GtidSet) obj;
            return this.map.equals(that.map);
        }
        return false;
    }

    @Override
    public String toString() {
        List<String> gtids = new ArrayList<>();
        for (UUIDSet uuidSet : map.values()) {
            gtids.add(uuidSet.toString());
        }
        return join(gtids, ",");
    }

    private static String join(Collection<?> o, String delimiter) {
        if (o.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (Object o1 : o) {
            sb.append(o1).append(delimiter);
        }
        return sb.substring(0, sb.length() - delimiter.length());
    }

    /**
     * A range of GTIDs for a single server with a specific UUID.
     * @see GtidSet
     */
    public static final class UUIDSet {

        private String uuid;

        private List<Interval> intervals;

        public UUIDSet() {
        }

        public UUIDSet(String uuid, List<Interval> intervals) {
            this.uuid = uuid;
            this.intervals = intervals;
            if (intervals.size() > 1) {
                joinAdjacentIntervals(0);
            }
        }

        public byte[] encode() throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();

            ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
            UUID uuidObj = UUID.fromString(uuid);
            bb.putLong(uuidObj.getMostSignificantBits());
            bb.putLong(uuidObj.getLeastSignificantBits());

            out.write(bb.array());

            ByteHelper.writeUnsignedInt64LittleEndian(intervals.size(), out);

            for (Interval interval : intervals) {
                ByteHelper.writeUnsignedInt64LittleEndian(interval.start, out);
                ByteHelper.writeUnsignedInt64LittleEndian(interval.end + 1, out);
            }

            return out.toByteArray();
        }

        public int decode(byte[] data) {
            return decode(data, 0);
        }

        public int decode(byte[] data, int index) {

            long mostSigBits = ByteBuffer.wrap(data, index,  8).getLong();
            index += 8;

            long leastSigBits = ByteBuffer.wrap(data, index,  8).getLong();
            index += 8;

            uuid = new UUID(mostSigBits, leastSigBits).toString();

            long intervalSize = ByteHelper.readUnsignedLongLittleEndian(data, index);
            index += 8;

            for (int i = 0; i < intervalSize; i++) {
                long start = ByteHelper.readUnsignedLongLittleEndian(data, index);
                index += 8;
                long end = ByteHelper.readUnsignedLongLittleEndian(data, index) - 1;
                index += 8;
                if (intervals == null) {
                    intervals = new ArrayList<>();
                }
                Interval interval = new Interval(start, end);
                intervals.add(interval);
            }

            return index;
        }

        private synchronized boolean add(long transactionId) {
            int index = findInterval(transactionId);
            boolean addedToExisting = false;
            if (index < intervals.size()) {
                Interval interval = intervals.get(index);
                if (interval.start == transactionId + 1) {
                    interval.start = transactionId;
                    addedToExisting = true;
                } else
                if (interval.end + 1 == transactionId) {
                    interval.end = transactionId;
                    addedToExisting = true;
                } else
                if (interval.start <= transactionId && transactionId <= interval.end) {
                    return false;
                }
            }
            if (!addedToExisting) {
                intervals.add(index, new Interval(transactionId, transactionId));
            }
            if (intervals.size() > 1) {
                joinAdjacentIntervals(index);
            }
            return true;
        }

        private synchronized boolean addAndFillGap(long transactionId) {
            if (intervals.size() == 0) {
                return add(transactionId);
            }
            int index = findInterval(transactionId);
            List<Interval> list = new ArrayList<>();
            if (index < intervals.size()) {
                if (intervals.get(index).getStart() > transactionId) {
                    index--;
                }
                if (index >= 0) {
                    list.add(new Interval(intervals.get(0).getStart(), Math.max(intervals.get(index).getEnd(), transactionId)));
                } else {
                    list.add(new Interval(transactionId, transactionId));
                }

            } else {
                list.add(new Interval(intervals.get(0).getStart(), transactionId));
            }
            for (int i = index + 1; i < intervals.size(); i++) {
                list.add(intervals.get(i));
            }
            intervals = list;

            if (intervals.size() > 1) {
                joinAdjacentIntervals(index);
            }
            return true;
        }

        /**
         * Collapses intervals like a-(b-1):b-c into a-c (only in index+-1 range).
         */
        private void joinAdjacentIntervals(int index) {
            for (int i = Math.min(index + 1, intervals.size() - 1), e = Math.max(index - 1, 0); i > e; i--) {
                Interval a = intervals.get(i - 1), b = intervals.get(i);
                if (a.end + 1 == b.start) {
                    a.end = b.end;
                    intervals.remove(i);
                }
            }
        }

        /**
         * @return index which is either a pointer to the interval containing v or a position at which v can be added
         */
        public int findInterval(long v) {
            int l = 0, p = 0, r = intervals.size();
            while (l < r) {
                p = (l + r) / 2;
                Interval i = intervals.get(p);
                if (i.end < v) {
                    l = p + 1;
                } else
                if (v < i.start) {
                    r = p;
                } else {
                    return p;
                }
            }
            if (!intervals.isEmpty() && intervals.get(p).end < v) {
                p++;
            }
            return p;
        }

        /**
         * Get the UUID for the server that generated the GTIDs.
         * @return the server's UUID; never null
         */
        public String getUUID() {
            return uuid;
        }

        /**
         * Get the intervals of transaction numbers.
         * @return the immutable transaction intervals; never null
         */
        public List<Interval> getIntervals() {
            return Collections.unmodifiableList(intervals);
        }

        public List<Interval> getMutableIntervals() {
            return intervals;
        }

        /**
         * Determine if the set of transaction numbers from this server is completely within the set of transaction
         * numbers from the set of transaction numbers in the supplied set.
         * @param other the set to compare with this set
         * @return {@code true} if this server's transaction numbers are equal to or a subset of the transaction
         * numbers of the supplied set, or false otherwise
         */
        public boolean isContainedWithin(UUIDSet other) {
            if (other == null) {
                return false;
            }
            if (!this.getUUID().equalsIgnoreCase(other.getUUID())) {
                // not even the same server ...
                return false;
            }
            if (this.intervals.isEmpty()) {
                return true;
            }
            if (other.intervals.isEmpty()) {
                return false;
            }
            // every interval in this must be within an interval of the other ...
            for (Interval thisInterval : this.intervals) {
                boolean found = false;
                for (Interval otherInterval : other.intervals) {
                    if (thisInterval.isContainedWithin(otherInterval)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return false; // didn't find a match
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            return uuid.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof UUIDSet) {
                UUIDSet that = (UUIDSet) obj;
                return this.getUUID().equalsIgnoreCase(that.getUUID()) &&
                        this.getIntervals().equals(that.getIntervals());
            }
            return super.equals(obj);
        }

        @Override
        public synchronized String toString() {
            StringBuilder sb = new StringBuilder();
            if (sb.length() != 0) {
                sb.append(',');
            }
            sb.append(uuid).append(':');
            Iterator<Interval> iter = intervals.iterator();
            if (iter.hasNext()) {
                sb.append(iter.next());
            }
            while (iter.hasNext()) {
                sb.append(':');
                sb.append(iter.next());
            }
            return sb.toString();
        }

        public UUIDSet getIntersection(UUIDSet other) {
            List<Interval> intervals1 = this.getIntervals();
            List<Interval> intervals2 = other.getIntervals();
            int i = 0, j = 0;
            List<Interval> res = Lists.newArrayList();
            while (i < intervals1.size() && j < intervals2.size()) {
                Interval interval1 = intervals1.get(i);
                Interval interval2 = intervals2.get(j);
                long start = Math.max(interval1.getStart(), interval2.getStart());
                long end = Math.min(interval1.getEnd(), interval2.getEnd());
                if (start <= end) {
                    res.add(new Interval(start, end));
                }
                if (interval1.getEnd() < interval2.getEnd()) {
                    i++;
                } else {
                    j++;
                }
            }
            return new UUIDSet(this.getUUID(), res);
        }
    }

    /**
     * An interval of contiguous transaction identifiers.
     * @see GtidSet
     */
    public static final class Interval implements Comparable<Interval> {

        private long start;
        private long end;

        public Interval(long start, long end) {
            this.start = start;
            this.end = end;
        }

        /**
         * Get the starting transaction number in this interval.
         * @return this interval's first transaction number
         */
        public long getStart() {
            return start;
        }

        /**
         * Get the ending transaction number in this interval.
         * @return this interval's last transaction number
         */
        public long getEnd() {
            return end;
        }

        /**
         * Determine if this interval is completely within the supplied interval.
         * @param other the interval to compare with
         * @return {@code true} if the {@link #getStart() start} is greater than or equal to the supplied interval's
         * {@link #getStart() start} and the {@link #getEnd() end} is less than or equal to the supplied
         * interval's {@link #getEnd() end}, or {@code false} otherwise
         */
        public boolean isContainedWithin(Interval other) {
            if (other == this) {
                return true;
            }
            if (other == null) {
                return false;
            }
            return this.getStart() >= other.getStart() && this.getEnd() <= other.getEnd();
        }

        @Override
        public int hashCode() {
            return (int) getStart();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof Interval) {
                Interval that = (Interval) obj;
                return this.getStart() == that.getStart() && this.getEnd() == that.getEnd();
            }
            return false;
        }

        @Override
        public String toString() {
            if (start == end) {
                return String.valueOf(start);
            }
            return start + "-" + end;
        }

        @Override
        public int compareTo(Interval o) {
            return saturatedCast(this.start - o.start);
        }

        private static int saturatedCast(long value) {
            if (value > Integer.MAX_VALUE) {
                return Integer.MAX_VALUE;
            }
            if (value < Integer.MIN_VALUE) {
                return Integer.MIN_VALUE;
            }
            return (int) value;
        }
    }

}

