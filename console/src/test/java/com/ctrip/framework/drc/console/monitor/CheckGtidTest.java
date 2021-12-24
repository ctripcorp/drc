package com.ctrip.framework.drc.console.monitor;

import com.ctrip.framework.drc.console.monitor.gtid.function.CheckGtid;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-03-03
 */
public class CheckGtidTest {

    private GtidSet gtidSet;

    private String uuid;

    private List<CheckGtid.GapInterval> gapIntervals;

    private CheckGtid checkGtid = new CheckGtid();

    @Before
    public void setUp() {
        String gtidSetStr = "uuid1:1-5:12-15:50-70, uuid2:101-110:118-120";
        gtidSet = new GtidSet(gtidSetStr);
        uuid = "uuid1";
        CheckGtid.GapInterval gapInterval1 = new CheckGtid.GapInterval(7L, 8L);
        CheckGtid.GapInterval gapInterval2 = new CheckGtid.GapInterval(30L, 40L);
        CheckGtid.GapInterval gapInterval3 = new CheckGtid.GapInterval(80L, 90L);
        gapIntervals = new LinkedList<>() {{
            add(gapInterval1);
            add(gapInterval2);
            add(gapInterval3);
        }};
    }

    @Test
    public void testGetRepeatedGapInterval() {
        GtidSet.UUIDSet uuidSet = gtidSet.getUUIDSet(uuid);
        System.out.println("uuid intervals: " + getGapCount(gtidSet.getUUIDSet(uuid).getIntervals()));
        System.out.println("uuid1's max tx number: " + getMaxTransaction(uuidSet));
        List<CheckGtid.GapInterval> repeatedGapInterval = getRepeatedGapInterval(uuid, gapIntervals, gtidSet);
        for(CheckGtid.GapInterval gapInterval : repeatedGapInterval) {
            System.out.println(gapInterval.toString());
        }
    }

    private long getGapCount(List<GtidSet.Interval> intervals) {
        int size = intervals.size();
        if(size < 2) {
            return 0;
        }
        long gapCount = 0;
        for(int i = 0; i < size - 1; ++i) {
            gapCount += (intervals.get(i+1).getStart() - intervals.get(i).getEnd() - 1);
        }
        return gapCount;
    }

    private List<CheckGtid.GapInterval> getRepeatedGapInterval(String uuid, List<CheckGtid.GapInterval> curGapIntervals, GtidSet gtidSet) {
        List<CheckGtid.GapInterval> repeatedGapIntervals = Lists.newArrayList();

        GtidSet.UUIDSet uuidSet = gtidSet.getUUIDSet(uuid);
        long maxTransaction = getMaxTransaction(uuidSet);
        for(CheckGtid.GapInterval gapInterval : curGapIntervals) {
            long start = gapInterval.getStart();
            long end = gapInterval.getEnd();
            Long repeatedGapStart = null, repeatedGapEnd = null;
            for(long transactionId = start; transactionId <= end; ++transactionId) {
                String gtid = uuid+":"+transactionId;
                if(gtidSet.add(gtid)) {
                    if(null == repeatedGapStart) {
                        repeatedGapStart = transactionId;
                        repeatedGapEnd = transactionId;
                    } else {
                        if(transactionId - repeatedGapEnd > 1 && repeatedGapStart < maxTransaction) {
                            repeatedGapIntervals.add(new CheckGtid.GapInterval(repeatedGapStart, repeatedGapEnd));
                            repeatedGapStart = transactionId;
                        }
                        repeatedGapEnd = transactionId;
                    }
                }
            }
            if(null != repeatedGapStart && repeatedGapStart < maxTransaction) {
                repeatedGapIntervals.add(new CheckGtid.GapInterval(repeatedGapStart, repeatedGapEnd));
            }
        }
        return repeatedGapIntervals;
    }

    private long getMaxTransaction(GtidSet.UUIDSet uuidSet) {
        List<GtidSet.Interval> intervals = uuidSet.getIntervals();
        long max = Long.MIN_VALUE;
        for(GtidSet.Interval interval : intervals) {
            long end = interval.getEnd();
            max = end > max ? end : max;
        }
        return max;
    }
}
