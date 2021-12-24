package com.ctrip.framework.drc.console.monitor.gtid.function;

import com.ctrip.framework.drc.console.mock.MockConfig;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-03-05
 */
public class CheckGtidTest {

    private CheckGtid checkGtid = new CheckGtid(new MockConfig());

    private String uuid;
    private List<GtidSet.Interval> intervals1;
    private List<GtidSet.Interval> intervals2;
    private List<GtidSet.Interval> intervals3;

    @Before
    public void setUp() {

        GtidSet gtidSet = new GtidSet("470f1e11-fafc-11e9-a234-fa163e6b26fd:1-311499,\n" +
                "80ec424f-faeb-11e9-922d-fa163eb4df21:1-59599535,\n" +
                "995cf799-fafc-11e9-a4ca-fa163ee96a2d:1-57812045:57812047-57812163");
        uuid = "995cf799-fafc-11e9-a4ca-fa163ee96a2d";
        intervals1 = new ArrayList<>() {{
            add(new GtidSet.Interval(1, 5));
            add(new GtidSet.Interval(7, 9));
            add(new GtidSet.Interval(11, 50));
        }};
        intervals2 = gtidSet.getUUIDSet(uuid).getIntervals();
        intervals3 = new ArrayList<>();
    }

    @Test
    public void testGetCurGtidSetAfterFilter() {
        String gtidSetStr = uuid + ":1-277006139:1878199780-1878199781";
        String gtidFilterSetStr = uuid + ":1878199780-1878199781";
        Assert.assertEquals(uuid + ":1-277006139", checkGtid.getCurGtidSetAfterFilter(gtidSetStr, gtidFilterSetStr).toString());

        gtidSetStr = uuid + ":1-277006139:1878199780-1878199782";
        gtidFilterSetStr = uuid + ":1878199780-1878199781";
        Assert.assertEquals(uuid + ":1-277006139:1878199780-1878199782", checkGtid.getCurGtidSetAfterFilter(gtidSetStr, gtidFilterSetStr).toString());

        gtidSetStr = uuid + ":1-100:200-300:400-500";
        gtidFilterSetStr = uuid + ":200-300:400-500";
        Assert.assertEquals(uuid + ":1-100", checkGtid.getCurGtidSetAfterFilter(gtidSetStr, gtidFilterSetStr).toString());

        gtidSetStr = uuid + ":1-300:400-500";
        gtidFilterSetStr = uuid + ":200-300:400-500";
        Assert.assertEquals(uuid + ":1-300", checkGtid.getCurGtidSetAfterFilter(gtidSetStr, gtidFilterSetStr).toString());

        gtidSetStr = uuid + ":1-398:400-500";
        gtidFilterSetStr = uuid + ":200-300:400-500";
        Assert.assertEquals(uuid + ":1-398", checkGtid.getCurGtidSetAfterFilter(gtidSetStr, gtidFilterSetStr).toString());

        gtidSetStr = uuid + ":1-500";
        gtidFilterSetStr = uuid + ":200-300:400-500";
        Assert.assertEquals(uuid + ":1-500", checkGtid.getCurGtidSetAfterFilter(gtidSetStr, gtidFilterSetStr).toString());

        gtidSetStr = uuid + ":1-505";
        gtidFilterSetStr = uuid + ":200-300:400-500";
        Assert.assertEquals(uuid + ":1-505", checkGtid.getCurGtidSetAfterFilter(gtidSetStr, gtidFilterSetStr).toString());

        gtidSetStr =  "uuid1:1-277006139:1878199780-1878199781,uuid2:1-50:60-100,uuid3:1-10:99-101";
        gtidFilterSetStr = "uuid1:1878199780-1878199781,uuid3:99-100,uuid4:1000-1001";
        Assert.assertEquals("uuid1:1-277006139,uuid2:1-50:60-100,uuid3:1-10:99-101", checkGtid.getCurGtidSetAfterFilter(gtidSetStr, gtidFilterSetStr).toString());
    }

    @Test
    public void testGetGtid() {
        String gtid = checkGtid.getGtid(uuid, intervals1);
        Assert.assertEquals("995cf799-fafc-11e9-a4ca-fa163ee96a2d:1-5:7-9:11-50", gtid);
    }

    @Test
    public void testGetGapCount() {
        long gapCount1 = checkGtid.getGapCount(intervals1);
        long gapCount2 = checkGtid.getGapCount(intervals2);
        long gapCount3 = checkGtid.getGapCount(intervals3);
        Assert.assertEquals(2, gapCount1);
        Assert.assertEquals(1, gapCount2);
        Assert.assertEquals(0, gapCount3);
    }

    @Test
    public void testGetGapIntervals() {
        List<GtidSet.Interval> intervals1 = new ArrayList<>() {{
            add(new GtidSet.Interval(1L, 277006139L));
            add(new GtidSet.Interval(1878199780L, 1878199781L));
        }};
        List<GtidSet.Interval> intervals2 = new ArrayList<>() {{
            add(new GtidSet.Interval(10L, 15L));
        }};
        List<GtidSet.Interval> intervals3 = new ArrayList<>();

        List<CheckGtid.GapInterval> expected1 = new LinkedList<>() {{
            add(new CheckGtid.GapInterval(277006140L, 1878199779L));
        }};
        List<CheckGtid.GapInterval> expected2 = new LinkedList<>();
        List<CheckGtid.GapInterval> expected3 = new LinkedList<>();

        Assert.assertEquals(expected1, checkGtid.getGapIntervals(intervals1));
        Assert.assertEquals(1, checkGtid.getGapIntervals(intervals1).size());
        Assert.assertEquals(expected2, checkGtid.getGapIntervals(intervals2));
        Assert.assertEquals(0, checkGtid.getGapIntervals(intervals2).size());
        Assert.assertEquals(expected3, checkGtid.getGapIntervals(intervals3));
        Assert.assertEquals(0, checkGtid.getGapIntervals(intervals3).size());
    }

    @Test
    public void testGetRepeatedGapInterval() {

        String uuid = "1e698014-90e5-11e9-a232-56d52b562cdd";
        List<CheckGtid.GapInterval> curGapIntervals1 = new LinkedList<>() {{
            add(new CheckGtid.GapInterval(277006140L, 277006999L));
        }};
        List<CheckGtid.GapInterval> curGapIntervals2 = new LinkedList<>() {{
            add(new CheckGtid.GapInterval(277006140L, 277006200L));
            add(new CheckGtid.GapInterval(277006205L, 277006210L));
        }};
        List<CheckGtid.GapInterval> curGapIntervals3 = new LinkedList<>() {{
            add(new CheckGtid.GapInterval(277006140L, 277007005L));
        }};
        List<CheckGtid.GapInterval> curGapIntervals4 = new LinkedList<>() {{
            add(new CheckGtid.GapInterval(277007005L, 277007010L));
        }};

        List<CheckGtid.GapInterval> expected1 = new LinkedList<>() {{
            add(new CheckGtid.GapInterval(277006140L, 277006999L));
        }};
        List<CheckGtid.GapInterval> expected2 = new LinkedList<>() {{
            add(new CheckGtid.GapInterval(277006140L, 277006200L));
            add(new CheckGtid.GapInterval(277006205L, 277006210L));
        }};
        List<CheckGtid.GapInterval> expected3 = new LinkedList<>() {{
            add(new CheckGtid.GapInterval(277006140L, 277006999L));
        }};
        List<CheckGtid.GapInterval> expected4 = new LinkedList<>();

        GtidSet gtidSet = new GtidSet("1e698014-90e5-11e9-a232-56d52b562abc:366561-370331:370732-374168,1e698014-90e5-11e9-a232-56d52b562cdd:1-277006139:277007000-277007001");
        Assert.assertEquals(expected1, checkGtid.getRepeatedGapInterval(uuid, curGapIntervals1, gtidSet));
        gtidSet = new GtidSet("1e698014-90e5-11e9-a232-56d52b562abc:366561-370331:370732-374168,1e698014-90e5-11e9-a232-56d52b562cdd:1-277006139:277007000-277007001");
        Assert.assertEquals(expected2, checkGtid.getRepeatedGapInterval(uuid, curGapIntervals2, gtidSet));
        gtidSet = new GtidSet("1e698014-90e5-11e9-a232-56d52b562abc:366561-370331:370732-374168,1e698014-90e5-11e9-a232-56d52b562cdd:1-277006139:277007000-277007001");
        Assert.assertEquals(expected3, checkGtid.getRepeatedGapInterval(uuid, curGapIntervals3, gtidSet));
        gtidSet = new GtidSet("1e698014-90e5-11e9-a232-56d52b562abc:366561-370331:370732-374168,1e698014-90e5-11e9-a232-56d52b562cdd:1-277006139:277007000-277007001");
        Assert.assertEquals(expected4, checkGtid.getRepeatedGapInterval(uuid, curGapIntervals4, gtidSet));
    }

    @Test
    public void testGetRepeatedGapCount() {
        List<CheckGtid.GapInterval> gapIntervals1 = new LinkedList<>() {{
            add(new CheckGtid.GapInterval(277006140L, 277006999L));
        }};
        List<CheckGtid.GapInterval> gapIntervals2 = new LinkedList<>() {{
            add(new CheckGtid.GapInterval(277006140L, 277006200L));
            add(new CheckGtid.GapInterval(277006205L, 277006210L));
        }};
        List<CheckGtid.GapInterval> gapIntervals3 = new LinkedList<>() {{
            add(new CheckGtid.GapInterval(277006140L, 277006999L));
        }};
        List<CheckGtid.GapInterval> gapIntervals4 = new LinkedList<>();

        Assert.assertEquals(860L, checkGtid.getRepeatedGapCount(gapIntervals1));
        Assert.assertEquals(67L, checkGtid.getRepeatedGapCount(gapIntervals2));
        Assert.assertEquals(860L, checkGtid.getRepeatedGapCount(gapIntervals3));
        Assert.assertEquals(0L, checkGtid.getRepeatedGapCount(gapIntervals4));
    }

    @Test
    public void testGetMaxTransactionId()  {
        String gtidSetStr = "1e698014-90e5-11e9-a232-56d52b562cdd:1-30:80-150:200-300,abcd1234-90e5-11e9-a232-56d52b56abcd:1-100:500-650";
        GtidSet gtidSet = new GtidSet(gtidSetStr);

        Assert.assertEquals(300L, (long) checkGtid.getMaxTransactionId(gtidSet, "1e698014-90e5-11e9-a232-56d52b562cdd"));
        Assert.assertEquals(650L, (long) checkGtid.getMaxTransactionId(gtidSet, "abcd1234-90e5-11e9-a232-56d52b56abcd"));
        Assert.assertNull(checkGtid.getMaxTransactionId(gtidSet, "no-such-gtid"));
    }
}
