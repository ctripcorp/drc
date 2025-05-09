package com.ctrip.framework.drc.core.driver.binlog.gtid;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet.Interval;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet.UUIDSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by mingdongli
 * 2019/9/24 上午10:29.
 */
public class GtidSetTest {

    private static final String UUID = "24bc7850-2c16-11e6-a073-0242ac110002";

    private static final String GTID_SET = UUID + ":1-15";

    private GtidSet gtidSet;

    @Before
    public void setUp() throws Exception {
        gtidSet = new GtidSet(GTID_SET);
    }

    @Test
    public void testNull() {
        GtidSet purgedGtidSet = new GtidSet("");
        boolean legal = purgedGtidSet.isContainedWithin(new GtidSet("b63f7e4a-a213-11ed-989f-02720d9a4dd2:1-16863592"));
        Assert.assertTrue(legal);
    }

    @Test
    public void encode() throws IOException {
        byte[] decoded = gtidSet.encode();
        GtidSet clone = new GtidSet(Maps.newLinkedHashMap());
        clone.decode(decoded);
        Assert.assertEquals(clone, gtidSet);
    }

    @Test
    public void encodeMax() throws IOException {
        GtidSet gtidSet = new GtidSet(UUID + ":" + Long.MAX_VALUE);
        byte[] decoded = gtidSet.encode();
        GtidSet clone = new GtidSet(Maps.newLinkedHashMap());
        clone.decode(decoded);
        Assert.assertEquals(clone, gtidSet);
    }


    @Test
    public void testAdd() throws Exception {
        GtidSet gtidSet = new GtidSet("00000000-0000-0000-0000-000000000000:3-5");
        gtidSet.add("00000000-0000-0000-0000-000000000000:2");
        gtidSet.add("00000000-0000-0000-0000-000000000000:4");
        gtidSet.add("00000000-0000-0000-0000-000000000000:5");
        gtidSet.add("00000000-0000-0000-0000-000000000000:7");
        gtidSet.add("00000000-0000-0000-0000-000000000001:9");
        gtidSet.add("00000000-0000-0000-0000-000000000000:0");
        Assert.assertEquals(gtidSet.toString(),
                "00000000-0000-0000-0000-000000000000:0:2-5:7,00000000-0000-0000-0000-000000000001:9");
    }


    @Test
    public void testAdd2() throws Exception {
        String uuid = "00000000-0000-0000-0000-000000000000";
        String uuid2 = "00000000-0000-0000-0000-000000000001";
        GtidSet gtidSet = new GtidSet(uuid + ":3-5");
        gtidSet.add(uuid, 2);
        gtidSet.add(uuid, 4);
        gtidSet.add(uuid, 5);
        gtidSet.add(uuid, 7);
        gtidSet.add(uuid2, 9);
        gtidSet.add(uuid, 0);
        Assert.assertEquals(gtidSet.toString(),
                uuid + ":0:2-5:7," + uuid2 + ":9");
    }

    @Test
    public void testAddUsingGtid() throws Exception {
        String uuid = "00000000-0000-0000-0000-000000000000";
        String uuid2 = "00000000-0000-0000-0000-000000000001";
        GtidSet gtidSet = new GtidSet(uuid + ":3-5");
        gtidSet.add(new Gtid(uuid, 2));
        gtidSet.add(new Gtid(uuid, 4));
        gtidSet.add(new Gtid(uuid, 5));
        gtidSet.add(new Gtid(uuid, 7));
        gtidSet.add(new Gtid(uuid2, 9));
        gtidSet.add(new Gtid(uuid, 0));
        Assert.assertEquals(gtidSet.toString(),
                uuid + ":0:2-5:7," + uuid2 + ":9");
    }

    @Test
    public void testAddAndFillGap(){
        GtidSet gtidSet;

        gtidSet = new GtidSet("");
        gtidSet.addAndFillGap("00000000-0000-0000-0000-000000000000:4");
        Assert.assertEquals("00000000-0000-0000-0000-000000000000:4", gtidSet.toString());


        gtidSet = new GtidSet("00000000-0000-0000-0000-000000000000:4");
        gtidSet.addAndFillGap("00000000-0000-0000-0000-000000000000:5");
        Assert.assertEquals("00000000-0000-0000-0000-000000000000:4-5", gtidSet.toString());

        gtidSet = new GtidSet("00000000-0000-0000-0000-000000000000:4");
        gtidSet.addAndFillGap("00000000-0000-0000-0000-000000000000:6");
        Assert.assertEquals("00000000-0000-0000-0000-000000000000:4-6", gtidSet.toString());


        gtidSet = new GtidSet("00000000-0000-0000-0000-000000000000:4");
        gtidSet.addAndFillGap("00000000-0000-0000-0000-000000000000:2");
        Assert.assertEquals("00000000-0000-0000-0000-000000000000:2:4", gtidSet.toString());



        gtidSet = new GtidSet("00000000-0000-0000-0000-000000000000:2:4");
        gtidSet.addAndFillGap("00000000-0000-0000-0000-000000000000:3");
        Assert.assertEquals("00000000-0000-0000-0000-000000000000:2-4", gtidSet.toString());

        gtidSet = new GtidSet("00000000-0000-0000-0000-000000000000:2-5");
        gtidSet.addAndFillGap("00000000-0000-0000-0000-000000000000:4");
        Assert.assertEquals("00000000-0000-0000-0000-000000000000:2-5", gtidSet.toString());


        gtidSet = new GtidSet("00000000-0000-0000-0000-000000000000:2-5");
        gtidSet.addAndFillGap("00000000-0000-0000-0000-000000000000:11");
        Assert.assertEquals("00000000-0000-0000-0000-000000000000:2-11", gtidSet.toString());

        gtidSet = new GtidSet("00000000-0000-0000-0000-000000000000:5-10");
        gtidSet.addAndFillGap("00000000-0000-0000-0000-000000000000:2");
        Assert.assertEquals("00000000-0000-0000-0000-000000000000:2:5-10", gtidSet.toString());


        gtidSet = new GtidSet("00000000-0000-0000-0000-000000000000:1-8:12-15:17-20");
        gtidSet.addAndFillGap("00000000-0000-0000-0000-000000000000:10");
        Assert.assertEquals("00000000-0000-0000-0000-000000000000:1-10:12-15:17-20", gtidSet.toString());


        gtidSet = new GtidSet("00000000-0000-0000-0000-000000000000:1-9:12-15:17-20");
        gtidSet.addAndFillGap("00000000-0000-0000-0000-000000000000:10");
        Assert.assertEquals("00000000-0000-0000-0000-000000000000:1-10:12-15:17-20", gtidSet.toString());

        gtidSet = new GtidSet("00000000-0000-0000-0000-000000000000:1-10:12-15:17-20");
        gtidSet.addAndFillGap("00000000-0000-0000-0000-000000000000:11");
        Assert.assertEquals("00000000-0000-0000-0000-000000000000:1-15:17-20", gtidSet.toString());

        gtidSet = new GtidSet("00000000-0000-0000-0000-000000000000:1-15:17-20");
        gtidSet.addAndFillGap("00000000-0000-0000-0000-000000000000:22");
        Assert.assertEquals("00000000-0000-0000-0000-000000000000:1-22", gtidSet.toString());
    }

    @Test
    public void testJoin() throws Exception {
        GtidSet gtidSet = new GtidSet("00000000-0000-0000-0000-000000000000:3-4:6-7");
        gtidSet.add("00000000-0000-0000-0000-000000000000:5");
        Assert.assertEquals(gtidSet.getUUIDSets().iterator().next().getIntervals().iterator().next().getEnd(), 7);
        Assert.assertEquals(gtidSet.toString(), "00000000-0000-0000-0000-000000000000:3-7");
    }

    @Test
    public void testEmptySet() throws Exception {
        Assert.assertEquals(new GtidSet("").toString(), "");
    }
    
 
    
    @Test
    public void testEquals() {
        Assert.assertEquals(new GtidSet(""), new GtidSet(Maps.newLinkedHashMap()));
        Assert.assertEquals(new GtidSet(""), new GtidSet(""));
        Assert.assertEquals(new GtidSet(UUID + ":1-191"), new GtidSet(UUID + ":1-191"));
        Assert.assertEquals(new GtidSet(UUID + ":1-191:192-199"), new GtidSet(UUID + ":1-191:192-199"));
        Assert.assertEquals(new GtidSet(UUID + ":1-191:192-199"), new GtidSet(UUID + ":1-199"));
        Assert.assertEquals(new GtidSet(UUID + ":1-191:193-199"), new GtidSet(UUID + ":1-191:193-199"));
        Assert.assertNotEquals(new GtidSet(UUID + ":1-191:193-199"), new GtidSet(UUID + ":1-199"));
    }

    @Test
    public void testSubsetOf() {
        GtidSet[] set = {
                new GtidSet(""),
                new GtidSet(UUID + ":1-191"),
                new GtidSet(UUID + ":192-199"),
                new GtidSet(UUID + ":1-191:192-199"),
                new GtidSet(UUID + ":1-191:193-199"),
                new GtidSet(UUID + ":2-199"),
                new GtidSet(UUID + ":1-200")
        };
        byte[][] subsetMatrix = {
                {1, 1, 1, 1, 1, 1, 1},
                {0, 1, 0, 1, 1, 0, 1},
                {0, 0, 1, 1, 0, 1, 1},
                {0, 0, 0, 1, 0, 0, 1},
                {0, 0, 0, 1, 1, 0, 1},
                {0, 0, 0, 1, 0, 1, 1},
                {0, 0, 0, 0, 0, 0, 1},
        };
        for (int i = 0; i < subsetMatrix.length; i++) {
            byte[] subset = subsetMatrix[i];
            for (int j = 0; j < subset.length; j++) {
                Assert.assertEquals(set[i].isContainedWithin(set[j]), subset[j] == 1);
            }
        }
    }

    @Test
    public void testSingleInterval() {
        GtidSet gtidSet = new GtidSet(UUID + ":1-191");
        UUIDSet uuidSet = gtidSet.getUUIDSet(UUID);
        Assert.assertEquals(uuidSet.getIntervals().size(), 1);
        Assert.assertEquals(uuidSet.getIntervals().iterator().next(), new Interval(1, 191));
        Assert.assertEquals(new LinkedList<Interval>(uuidSet.getIntervals()).getLast(), new Interval(1, 191));
        Assert.assertEquals(gtidSet.toString(), UUID + ":1-191");
    }

    @Test
    public void testCollapseAdjacentIntervals() {
        GtidSet gtidSet = new GtidSet(UUID + ":1-191:192-199");
        UUIDSet uuidSet = gtidSet.getUUIDSet(UUID);
        Assert.assertEquals(uuidSet.getIntervals().size(), 1);
        Assert.assertTrue(uuidSet.getIntervals().contains(new Interval(1, 199)));
        Assert.assertEquals(uuidSet.getIntervals().iterator().next(), new Interval(1, 199));
        Assert.assertEquals(new LinkedList<Interval>(uuidSet.getIntervals()).getLast(), new Interval(1, 199));
        Assert.assertEquals(gtidSet.toString(), UUID + ":1-199");
    }

    @Test
    public void testNotCollapseNonAdjacentIntervals() {
        GtidSet gtidSet = new GtidSet(UUID + ":1-191:193-199");
        UUIDSet uuidSet = gtidSet.getUUIDSet(UUID);
        Assert.assertEquals(uuidSet.getIntervals().size(), 2);
        Assert.assertEquals(uuidSet.getIntervals().iterator().next(), new Interval(1, 191));
        Assert.assertEquals(new LinkedList<Interval>(uuidSet.getIntervals()).getLast(), new Interval(193, 199));
        Assert.assertEquals(gtidSet.toString(), UUID + ":1-191:193-199");
    }

    @Test
    public void testMultipleIntervals() {
        GtidSet set = new GtidSet(UUID + ":1-191:193-199:1000-1033");
        UUIDSet uuidSet = set.getUUIDSet(UUID);
        Assert.assertEquals(uuidSet.getIntervals().size(), 3);
        Assert.assertTrue(uuidSet.getIntervals().contains(new Interval(193, 199)));
        Assert.assertEquals(uuidSet.getIntervals().iterator().next(), new Interval(1, 191));
        Assert.assertEquals(new LinkedList<Interval>(uuidSet.getIntervals()).getLast(), new Interval(1000, 1033));
        Assert.assertEquals(set.toString(), UUID + ":1-191:193-199:1000-1033");
    }

    @Test
    public void testMultipleIntervalsThatMayBeAdjacent() {
        GtidSet gtidSet = new GtidSet(UUID + ":1-191:192-199:1000-1033:1035-1036:1038-1039");
        UUIDSet uuidSet = gtidSet.getUUIDSet(UUID);
        Assert.assertEquals(uuidSet.getIntervals().size(), 4);
        Assert.assertTrue(uuidSet.getIntervals().contains(new Interval(1000, 1033)));
        Assert.assertTrue(uuidSet.getIntervals().contains(new Interval(1035, 1036)));
        Assert.assertEquals(uuidSet.getIntervals().iterator().next(), new Interval(1, 199));
        Assert.assertEquals(new LinkedList<GtidSet.Interval>(uuidSet.getIntervals()).getLast(), new Interval(1038, 1039));
        Assert.assertEquals(gtidSet.toString(), UUID + ":1-199:1000-1033:1035-1036:1038-1039");
    }

    @Test
    public void testPutUUIDSet() {
        GtidSet gtidSet = new GtidSet(UUID + ":1-191");
        GtidSet gtidSet2 = new GtidSet(UUID + ":1-190");
        UUIDSet uuidSet2 = gtidSet2.getUUIDSet(UUID);
        gtidSet.putUUIDSet(uuidSet2);
        Assert.assertEquals(gtidSet, gtidSet2);
    }

    @Test
    public void testClone() {
        GtidSet set = new GtidSet(UUID + ":1-191:193-199:1000-1033");
        GtidSet clone = set.clone();
        Assert.assertEquals(set, clone);
    }

    @Test
    public void testSame() {
        String gtidString = UUID + ":1";
        GtidSet set = new GtidSet(gtidString);
        String setString = set.toString();
        Assert.assertEquals(gtidString, setString);
    }

    @Test
    public void testSubtract() {
        GtidSet big = new GtidSet(UUID + ":1-10");
        GtidSet small = new GtidSet(UUID + ":3-8");
        GtidSet res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":1-2:9-10");

        big = new GtidSet(UUID + ":1-10:50-100");
        small = new GtidSet(UUID + ":3-8:75-85:98");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":1-2:9-10:50-74:86-97:99-100");
        res = small.subtract(big);
        Assert.assertEquals(res.toString(), "");

        big = new GtidSet(UUID + ":1-10:100");
        small = new GtidSet(UUID + ":100");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":1-10");
        res = small.subtract(big);
        Assert.assertEquals(res.toString(), "");

        big = new GtidSet("");
        small = new GtidSet(UUID + ":3-8:75-85:98");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), "");

        big = new GtidSet(UUID + ":1-10:50-100");
        small = new GtidSet(UUID + ":3-8:75-85:98-99");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":1-2:9-10:50-74:86-97:100");

        big = new GtidSet(UUID + ":15-200");
        small = new GtidSet(UUID + ":3-50:100:150-180");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":51-99:101-149:181-200");

        big = new GtidSet(UUID + ":1-10:15-20");
        small = new GtidSet(UUID + ":3-12");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":1-2:15-20");

        big = new GtidSet(UUID + ":1-10:15-20");
        small = new GtidSet(UUID + ":3-17");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":1-2:18-20");

        big = new GtidSet(UUID + ":1-10");
        small = new GtidSet(UUID + ":3-17");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":1-2");

        big = new GtidSet(UUID + ":1-10:15-20");
        small = new GtidSet(UUID + ":3-22");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":1-2");

        big = new GtidSet(UUID + ":1-10:15-20:35-36:38-42");
        small = new GtidSet(UUID + ":3-40");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":1-2:41-42");


        big = new GtidSet(UUID + ":1-10");
        small = new GtidSet(UUID + ":1-10");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), "");

        big = new GtidSet(UUID + ":1-10");
        small = new GtidSet(UUID + ":1-9");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":10");

        big = new GtidSet(UUID + ":1-10");
        small = new GtidSet(UUID + ":2-10");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":1");

        big = new GtidSet(UUID + ":1-10");
        small = new GtidSet(UUID + ":2-11");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":1");

        big = new GtidSet(UUID + ":1-10");
        small = new GtidSet("");
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), UUID + ":1-10");

        String gtidSetStringBig = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1-2172782,e7d82d84-036c-11ea-bb09-075284a09713:1-427";
        String gtidSetStringSmall = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:2-2172778,e7d82d84-036c-11ea-bb09-075284a09713:2-427,3f40568c-6364-11ea-98b4-fa163ec90ff6:1-3";
        big = new GtidSet(gtidSetStringBig);
        small = new GtidSet(gtidSetStringSmall);
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1:2172779-2172782,e7d82d84-036c-11ea-bb09-075284a09713:1");

        gtidSetStringBig = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:2-2172778,e7d82d84-036c-11ea-bb09-075284a09713:2-427,3f40568c-6364-11ea-98b4-fa163ec90ff6:1-3";
        gtidSetStringSmall = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1-2172782,e7d82d84-036c-11ea-bb09-075284a09713:1-427";
        big = new GtidSet(gtidSetStringBig);
        small = new GtidSet(gtidSetStringSmall);
        res = big.subtract(small);
        Assert.assertEquals(res.toString(), "3f40568c-6364-11ea-98b4-fa163ec90ff6:1-3");
    }

    @Test
    public void testReplace() {
        String uuid = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe";
        String gtidSetStringSlave = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1-2172785,e7d82d84-036c-11ea-bb09-075284a09713:1-427";
        String gtidSetStringMaster = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1-2172782,e7d82d84-036c-11ea-bb09-075284a09713:1-429";
        GtidSet slave = new GtidSet(gtidSetStringSlave);
        GtidSet master = new GtidSet(gtidSetStringMaster);
        GtidSet newSlave = slave.replaceGtid(master, uuid);
        Assert.assertEquals(newSlave.toString(), "e7d82d84-036c-11ea-bb09-075284a09713:1-427,56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1-2172782");

        newSlave = slave.replaceGtid(master, "");
        Assert.assertEquals(newSlave.toString(), slave.toString());
    }

    @Test
    public void test1() {
        Assert.assertTrue(new GtidSet("b207f82e-2a7b-11ec-b128-1c34da51a830:1-25428879535").isContainedWithin(new GtidSet("b207f82e-2a7b-11ec-b128-1c34da51a830:1-25326877444:25326877445-25532555232")));
    }

    @Test
    public void testFilter() {
        Set<String> uuidSet = Sets.newHashSet("68226208-9374-11ea-819b-fa163e02998c", "02878c56-9375-11ea-b1c4-fa163eaa9d69", "dd3ccf94-9371-11ea-9f41-fa163ec90ff6");
        String gtidSetStringSlave = "02878c56-9375-11ea-b1c4-fa163eaa9d69:1-350744,106cab99-95c0-11ea-9ebe-fa163ec90ff6:1-4731";
        String gtidSetStringMaster = "68226208-9374-11ea-819b-fa163e02998c:1-349543,02878c56-9375-11ea-b1c4-fa163eaa9d69:1-371942,dd3ccf94-9371-11ea-9f41-fa163ec90ff6:1-580";
        GtidSet slave = new GtidSet(gtidSetStringSlave);
        GtidSet filtered = slave.filterGtid(uuidSet);
        GtidSet master = new GtidSet(gtidSetStringMaster);
        boolean res = filtered.isContainedWithin(master);
        Assert.assertEquals(res, true);
    }

    @Test
    public void testIntersectionGtidSet() {
        String current = "24b9c5bc-070f-11ec-aa01-b8cef6507418:153148495-153445256,c4fff537-2a2a-11eb-aae0-506b4b4791b4:1855122466-1855571751,c8331da3-512d-11e9-b435-48df3717a518:1068450202-1068695137,9c26dd63-3709-11ec-af66-1c34da7c121a:1881903-2107029,47e5a666-3708-11ec-895b-0c42a1002ff0:1747429-1969043";
        String executed = "34b4ecc5-3675-11ea-a598-b8599ffdbbb4:1-362422,5e279430-512e-11e9-b439-48df3717a524:1-128957648,9c26dd63-3709-11ec-af66-1c34da7c121a:1-13500738,c17c9fa6-c322-11e9-a0a2-98039bad5d88:1-13870,c200a3d7-3131-11ea-b1e7-e4434b6b0ae0:1-1198619808,c4fff537-2a2a-11eb-aae0-506b4b4791b4:1-1853256173:1853256176-1936745715,935066db-454b-11eb-bcfe-506b4b2af01e:1-271601355";
        GtidSet currentGtidSet = new GtidSet(current);
        GtidSet executeGtidSetd = new GtidSet(executed);
        Assert.assertEquals(currentGtidSet.isContainedWithin(executeGtidSetd), false);

        currentGtidSet = currentGtidSet.getIntersectionUUIDs(executeGtidSetd);
        Assert.assertEquals(currentGtidSet.isContainedWithin(executeGtidSetd), true);
        System.out.println(currentGtidSet);
    }

    @Test
    public void testIntersectionAll() {
        String current = "24b9c5bc-070f-11ec-aa01-b8cef6507418:153148495-153445256,c4fff537-2a2a-11eb-aae0-506b4b4791b4:1-5";
        String executed = "34b4ecc5-3675-11ea-a598-b8599ffdbbb4:1-362422,c4fff537-2a2a-11eb-aae0-506b4b4791b4:2-4:6-7";
        GtidSet currentGtidSet = new GtidSet(current);
        GtidSet executeGtidSetd = new GtidSet(executed);
        GtidSet result = currentGtidSet.getIntersection(executeGtidSetd);
        Assert.assertEquals(new GtidSet("c4fff537-2a2a-11eb-aae0-506b4b4791b4:2-4"), result);
    }

    @Test
    public void testIntersectionList() {
        List<GtidSet> list = Lists.newArrayList(
                new GtidSet("a:1-10,b:1-10"),
                new GtidSet("a:1-5:7-9,b:7-11"),
                new GtidSet("a:1-8,b:6-8")
        );

        Assert.assertNotEquals(new GtidSet("a:1-5:7-8,b:7-8,c:1-10"), GtidSet.getIntersection(list));
        Assert.assertEquals(new GtidSet("a:1-5:7-8,b:7-8"), GtidSet.getIntersection(list));
    }

    @Test
    public void testGetIntersectionNotSameObject() {
        GtidSet origin = new GtidSet("a:1-10");
        List<GtidSet> list = Lists.newArrayList(
                origin
        );

        GtidSet intersection = GtidSet.getIntersection(list);
        Assert.assertNotSame(intersection, origin);
    }

    @Test
    public void testUnion() {
        GtidSet big = new GtidSet(UUID + ":1-10:100");
        GtidSet small = new GtidSet(UUID + ":3-8");
        GtidSet res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-10:100");

        small = new GtidSet(UUID + ":2-5");
        big = new GtidSet(UUID + ":7-15:100");
        res = small.union(big);
        Assert.assertEquals(res.toString(), UUID + ":2-5:7-15:100");

        big = new GtidSet(UUID + ":7-15:100");
        small = new GtidSet(UUID + ":2-5");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":2-5:7-15:100");

        big = new GtidSet(UUID + ":1-3:6-10:15-20");
        small = new GtidSet(UUID + ":17-50:100");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-3:6-10:15-50:100");

        big = new GtidSet(UUID + ":1-3:6-10:15-20:100");
        small = new GtidSet(UUID + ":17-50");
        res = small.union(big);
        Assert.assertEquals(res.toString(), UUID + ":1-3:6-10:15-50:100");

        big = new GtidSet(UUID + ":1-10:50-100:1000");
        small = new GtidSet(UUID + ":3-8:75-85:98-110:120-140");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-10:50-110:120-140:1000");

        big = new GtidSet(UUID + ":1-10:50-100:1000");
        small = new GtidSet(UUID + ":3-8:75-85:98-110:120-140");
        res = small.union(big);
        Assert.assertEquals(res.toString(), UUID + ":1-10:50-110:120-140:1000");

        big = new GtidSet(UUID + ":15-200:1000");
        small = new GtidSet(UUID + ":3-50:100:150-180");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":3-200:1000");

        big = new GtidSet(UUID + ":15-200");
        small = new GtidSet(UUID + ":3-50:100:150-180:1000");
        res = small.union(big);
        Assert.assertEquals(res.toString(), UUID + ":3-200:1000");

        big = new GtidSet(UUID + ":1-10:15-20:1000");
        small = new GtidSet(UUID + ":1-12");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-12:15-20:1000");

        big = new GtidSet(UUID + ":1-10:15-20:1000");
        small = new GtidSet(UUID + ":1-12");
        res = small.union(big);
        Assert.assertEquals(res.toString(), UUID + ":1-12:15-20:1000");

        big = new GtidSet(UUID + ":1-10:15-20:1000");
        small = new GtidSet(UUID + ":3-17");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-20:1000");

        big = new GtidSet(UUID + ":1-10:15-20:1000");
        small = new GtidSet(UUID + ":3-17");
        res = small.union(big);
        Assert.assertEquals(res.toString(), UUID + ":1-20:1000");

        big = new GtidSet(UUID + ":1-10:1000");
        small = new GtidSet(UUID + ":3-17");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-17:1000");

        big = new GtidSet(UUID + ":1-10:1000");
        small = new GtidSet(UUID + ":3-17");
        res = small.union(big);
        Assert.assertEquals(res.toString(), UUID + ":1-17:1000");

        big = new GtidSet(UUID + ":1-10:15-20:1000");
        small = new GtidSet(UUID + ":3-22");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-22:1000");

        big = new GtidSet(UUID + ":1-10:15-20:1000");
        small = new GtidSet(UUID + ":3-22");
        res = small.union(big);
        Assert.assertEquals(res.toString(), UUID + ":1-22:1000");

        big = new GtidSet(UUID + ":1-10:15-20:35-36:38-42:1000");
        small = new GtidSet(UUID + ":3-40");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-42:1000");

        big = new GtidSet(UUID + ":1-10:15-20:35-36:38-42:1000");
        small = new GtidSet(UUID + ":3-40");
        res = small.union(big);
        Assert.assertEquals(res.toString(), UUID + ":1-42:1000");


        big = new GtidSet(UUID + ":1-10:1000");
        small = new GtidSet(UUID + ":1-10");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-10:1000");

        big = new GtidSet(UUID + ":1-10:1000");
        small = new GtidSet(UUID + ":1-9");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-10:1000");

        big = new GtidSet(UUID + ":1-10:1000");
        small = new GtidSet(UUID + ":2-10");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-10:1000");

        big = new GtidSet(UUID + ":1-10:1000");
        small = new GtidSet(UUID + ":2-11");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-11:1000");

        big = new GtidSet(UUID + ":1-10");
        small = new GtidSet(UUID + ":11-20");
        res = small.union(big);
        Assert.assertEquals(res.toString(), UUID + ":1-20");

        big = new GtidSet(UUID + ":1-10:1000");
        small = new GtidSet("");
        res = big.union(small);
        Assert.assertEquals(res.toString(), UUID + ":1-10:1000");

        big = new GtidSet(UUID + ":1-10:1000");
        small = new GtidSet("");
        res = small.union(big);
        Assert.assertEquals(res.toString(), UUID + ":1-10:1000");

        big = new GtidSet("");
        small = new GtidSet("");
        res = small.union(big);
        Assert.assertEquals(res.toString(), "");

        big = new GtidSet("cb190774-6bf1-11ea-9799-fa163e02998c:1-18912721");
        small = new GtidSet("cb190774-6bf1-11ea-9799-fa163e02998c:1-16504165:16504173-16541256:16541260-16545608:16545610-18913165");
        res = small.union(big);
        Assert.assertEquals(res.toString(), "cb190774-6bf1-11ea-9799-fa163e02998c:1-18913165");

        big = new GtidSet("cb190774-6bf1-11ea-9799-fa163e02998c:1-18912721,bb190774-6bf1-11ea-9799-fa163e02998c:1-18912");
        small = new GtidSet("cb190774-6bf1-11ea-9799-fa163e02998c:1-18912732,cb190774-6bf1-11ea-9799-fa163e029911:1-189127");
        res = small.union(big);
        Assert.assertEquals(res.toString(), "cb190774-6bf1-11ea-9799-fa163e02998c:1-18912732,cb190774-6bf1-11ea-9799-fa163e029911:1-189127,bb190774-6bf1-11ea-9799-fa163e02998c:1-18912");

        String gtidSetStringBig = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1-2172782,e7d82d84-036c-11ea-bb09-075284a09713:1-427";
        String gtidSetStringSmall = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:2-2172778,e7d82d84-036c-11ea-bb09-075284a09713:2-427,3f40568c-6364-11ea-98b4-fa163ec90ff6:1-3";
        big = new GtidSet(gtidSetStringBig);
        small = new GtidSet(gtidSetStringSmall);
        res = big.union(small);
        Assert.assertEquals(res.toString(), "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1-2172782,e7d82d84-036c-11ea-bb09-075284a09713:1-427,3f40568c-6364-11ea-98b4-fa163ec90ff6:1-3");

        gtidSetStringBig = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:2-2172778,e7d82d84-036c-11ea-bb09-075284a09713:2-427,3f40568c-6364-11ea-98b4-fa163ec90ff6:1-3";
        gtidSetStringSmall = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1-2172782,e7d82d84-036c-11ea-bb09-075284a09713:1-427";
        big = new GtidSet(gtidSetStringBig);
        small = new GtidSet(gtidSetStringSmall);
        res = big.union(small);
        Assert.assertEquals(res.toString(), "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1-2172782,e7d82d84-036c-11ea-bb09-075284a09713:1-427,3f40568c-6364-11ea-98b4-fa163ec90ff6:1-3");
    }


    @Test
    public void testUnionInPlace() {
        GtidSet big = new GtidSet(UUID + ":1-10:100");
        GtidSet small = new GtidSet(UUID + ":3-8");
        big.unionInPlace(small);
        Assert.assertEquals(big.toString(), UUID + ":1-10:100");

        small = new GtidSet(UUID + ":2-5");
        big = new GtidSet(UUID + ":7-15:100");
        small.unionInPlace(big);
        Assert.assertEquals(small.toString(), UUID + ":2-5:7-15:100");

        big = new GtidSet(UUID + ":7-15:100");
        small = new GtidSet(UUID + ":2-5");
        big.unionInPlace(small);
        Assert.assertEquals(big.toString(), UUID + ":2-5:7-15:100");

        big = new GtidSet(UUID + ":1-3:6-10:15-20");
        small = new GtidSet(UUID + ":17-50:100");
        big.unionInPlace(small);
        Assert.assertEquals(big.toString(), UUID + ":1-3:6-10:15-50:100");

        big = new GtidSet(UUID + ":1-3:6-10:15-20:100");
        small = new GtidSet(UUID + ":17-50");
        small.unionInPlace(big);
        Assert.assertEquals(small.toString(), UUID + ":1-3:6-10:15-50:100");

        big = new GtidSet(UUID + ":1-10:50-100:1000");
        small = new GtidSet(UUID + ":3-8:75-85:98-110:120-140");
        big.unionInPlace(small);
        Assert.assertEquals(big.toString(), UUID + ":1-10:50-110:120-140:1000");

        big = new GtidSet(UUID + ":1-10:50-100:1000");
        small = new GtidSet(UUID + ":3-8:75-85:98-110:120-140");
        small.unionInPlace(big);
        Assert.assertEquals(small.toString(), UUID + ":1-10:50-110:120-140:1000");

        big = new GtidSet(UUID + ":15-200:1000");
        small = new GtidSet(UUID + ":3-50:100:150-180");
        big.unionInPlace(small);
        Assert.assertEquals(big.toString(), UUID + ":3-200:1000");

        big = new GtidSet(UUID + ":15-200");
        small = new GtidSet(UUID + ":3-50:100:150-180:1000");
        small.unionInPlace(big);
        Assert.assertEquals(small.toString(), UUID + ":3-200:1000");

        big = new GtidSet(UUID + ":1-10:15-20:1000");
        small = new GtidSet(UUID + ":1-12");
        big.unionInPlace(small);
        Assert.assertEquals(big.toString(), UUID + ":1-12:15-20:1000");

        big = new GtidSet(UUID + ":1-10:15-20:1000");
        small = new GtidSet(UUID + ":1-12");
        small.unionInPlace(big);
        Assert.assertEquals(small.toString(), UUID + ":1-12:15-20:1000");

        big = new GtidSet(UUID + ":1-10:15-20:1000");
        small = new GtidSet(UUID + ":3-17");
        big.unionInPlace(small);
        Assert.assertEquals(big.toString(), UUID + ":1-20:1000");

        big = new GtidSet(UUID + ":1-10:15-20:1000");
        small = new GtidSet(UUID + ":3-17");
        small.unionInPlace(big);
        Assert.assertEquals(small.toString(), UUID + ":1-20:1000");

        big = new GtidSet(UUID + ":1-10:1000");
        small = new GtidSet(UUID + ":3-17");
        big.unionInPlace(small);
        Assert.assertEquals(big.toString(), UUID + ":1-17:1000");

        big = new GtidSet(UUID + ":1-10:1000");
        small = new GtidSet(UUID + ":3-17");
        small.unionInPlace(big);
        Assert.assertEquals(small.toString(), UUID + ":1-17:1000");

        big = new GtidSet(UUID + ":1-10:15-20:1000");
        small = new GtidSet(UUID + ":3-22");
        big.unionInPlace(small);
        Assert.assertEquals(big.toString(), UUID + ":1-22:1000");

        big = new GtidSet(UUID + ":1-10:15-20:1000");
        small = new GtidSet(UUID + ":3-22");
        small.unionInPlace(big);
        Assert.assertEquals(small.toString(), UUID + ":1-22:1000");

        big = new GtidSet(UUID + ":1-10:15-20:35-36:38-42:1000");
        small = new GtidSet(UUID + ":3-40");
        big.unionInPlace(small);
        Assert.assertEquals(big.toString(), UUID + ":1-42:1000");

        big = new GtidSet(UUID + ":1-10:15-20:35-36:38-42:1000");
        small = new GtidSet(UUID + ":3-40");
        small.unionInPlace(big);
        Assert.assertEquals(small.toString(), UUID + ":1-42:1000");


        big = new GtidSet(UUID + ":1-10:1000");
        small = new GtidSet(UUID + ":1-10");
        big.unionInPlace(small);
        Assert.assertEquals(big.toString(), UUID + ":1-10:1000");

        big = new GtidSet(UUID + ":1-10:1000");
        small = new GtidSet(UUID + ":1-9");
        big.unionInPlace(small);
        Assert.assertEquals(big.toString(), UUID + ":1-10:1000");

        big = new GtidSet(UUID + ":1-10:1000");
        small = new GtidSet(UUID + ":2-10");
        big.unionInPlace(small);
        Assert.assertEquals(big.toString(), UUID + ":1-10:1000");

        big = new GtidSet(UUID + ":1-10:1000");
        small = new GtidSet(UUID + ":2-11");
        big.unionInPlace(small);
        Assert.assertEquals(big.toString(), UUID + ":1-11:1000");

        big = new GtidSet(UUID + ":1-10");
        small = new GtidSet(UUID + ":11-20");
        small.unionInPlace(big);
        Assert.assertEquals(small.toString(), UUID + ":1-20");

        big = new GtidSet(UUID + ":1-10:1000");
        small = new GtidSet("");
        big.unionInPlace(small);
        Assert.assertEquals(big.toString(), UUID + ":1-10:1000");

        big = new GtidSet(UUID + ":1-10:1000");
        small = new GtidSet("");
        small.unionInPlace(big);
        Assert.assertEquals(small.toString(), UUID + ":1-10:1000");

        big = new GtidSet("");
        small = new GtidSet("");
        small.unionInPlace(big);
        Assert.assertEquals(small.toString(), "");

        big = new GtidSet("cb190774-6bf1-11ea-9799-fa163e02998c:1-18912721");
        small = new GtidSet("cb190774-6bf1-11ea-9799-fa163e02998c:1-16504165:16504173-16541256:16541260-16545608:16545610-18913165");
        small.unionInPlace(big);
        Assert.assertEquals(small.toString(), "cb190774-6bf1-11ea-9799-fa163e02998c:1-18913165");

        big = new GtidSet("cb190774-6bf1-11ea-9799-fa163e02998c:1-18912721,bb190774-6bf1-11ea-9799-fa163e02998c:1-18912");
        small = new GtidSet("cb190774-6bf1-11ea-9799-fa163e02998c:1-18912732,cb190774-6bf1-11ea-9799-fa163e029911:1-189127");
        small.unionInPlace(big);
        Assert.assertEquals(small.toString(), "cb190774-6bf1-11ea-9799-fa163e02998c:1-18912732,cb190774-6bf1-11ea-9799-fa163e029911:1-189127,bb190774-6bf1-11ea-9799-fa163e02998c:1-18912");

        String gtidSetStringBig = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1-2172782,e7d82d84-036c-11ea-bb09-075284a09713:1-427";
        String gtidSetStringSmall = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:2-2172778,e7d82d84-036c-11ea-bb09-075284a09713:2-427,3f40568c-6364-11ea-98b4-fa163ec90ff6:1-3";
        big = new GtidSet(gtidSetStringBig);
        small = new GtidSet(gtidSetStringSmall);
        big.unionInPlace(small);
        Assert.assertEquals(big.toString(), "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1-2172782,e7d82d84-036c-11ea-bb09-075284a09713:1-427,3f40568c-6364-11ea-98b4-fa163ec90ff6:1-3");

        gtidSetStringBig = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:2-2172778,e7d82d84-036c-11ea-bb09-075284a09713:2-427,3f40568c-6364-11ea-98b4-fa163ec90ff6:1-3";
        gtidSetStringSmall = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1-2172782,e7d82d84-036c-11ea-bb09-075284a09713:1-427";
        big = new GtidSet(gtidSetStringBig);
        small = new GtidSet(gtidSetStringSmall);
        big.unionInPlace(small);
        Assert.assertEquals(big.toString(), "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1-2172782,e7d82d84-036c-11ea-bb09-075284a09713:1-427,3f40568c-6364-11ea-98b4-fa163ec90ff6:1-3");
    }


    @Test
    public void testConcurrentWhenAddInterval() throws InterruptedException {
        AtomicReference<Exception> exception = new AtomicReference<>(null);
        String gtidSetString = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1-20";
        GtidSet gtidSet = new GtidSet(gtidSetString);
        Thread thread1 = new Thread(() -> {
            for (int i = 21; i < 100; ++i) {
                try {
                    gtidSet.clone();
                } catch (Exception e) {
                    exception.set(e);
                    break;
                }
            }
        });

        Thread thread2 = new Thread(() -> {
            for (int i = 22; i < 100; i = i + 2) {
                gtidSet.add("56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:" + i);
            }
        });
        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        if (exception.get() != null && exception.get() instanceof ConcurrentModificationException) {
            Assert.fail();
        }
    }

    @Test
    public void testUnionFakeGap() {
        String gtidSetSelectFromDb = "b207f82e-2a7b-11ec-b128-1c34da51a830:25326877444:25326877987";
        String gtidSetInMemory = "b207f82e-2a7b-11ec-b128-1c34da51a830:25326877439-25326877441:25326877443:25326877445-25326887501";
        GtidSet gtidSet = new GtidSet(gtidSetInMemory);
        String gtidSetToUpdate = new GtidSet(gtidSetSelectFromDb).union(gtidSet).getUUIDSet("b207f82e-2a7b-11ec-b128-1c34da51a830").toString();
        Assert.assertEquals(gtidSetToUpdate, "b207f82e-2a7b-11ec-b128-1c34da51a830:25326877439-25326877441:25326877443-25326887501");
    }

    @Test
    public void testIsContainedWithin() {
        GtidSet gtidSet = new GtidSet("");

        String gtid = null;
        Assert.assertFalse(gtidSet.isContainedWithin(gtid));

        gtid = "b207f82e-2a7b-11ec-b128-1c34da51a830:25326877444";
        Assert.assertFalse(gtidSet.isContainedWithin(gtid));

        gtidSet.add(gtid);
        Assert.assertTrue(gtidSet.isContainedWithin(gtid));

        gtid = "b207f82e-2a7b-11ec-b128-1c34da51a830:25326877445";
        Assert.assertFalse(gtidSet.isContainedWithin(gtid));

        gtidSet.add(gtid);
        Assert.assertTrue(gtidSet.isContainedWithin(gtid));

        gtidSet = new GtidSet("b207f82e-2a7b-11ec-b128-1c34da51a830:25326877445,56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:2");
        Assert.assertTrue(gtidSet.isContainedWithin(gtid));
    }

    @Test
    public void testExpandTo() {
        GtidSet expected = new GtidSet("");

        String gtid = null;
        expected.expandTo(gtid);
        Assert.assertEquals(new GtidSet("").toString(), expected.toString());

        gtid = "b207f82e-2a7b-11ec-b128-1c34da51a830:25326877444";
        expected.expandTo(gtid);
        Assert.assertEquals(new GtidSet(gtid).toString(), expected.toString());

        gtid = "b207f82e-2a7b-11ec-b128-1c34da51a830:25326877445";
        expected.expandTo(gtid);
        Assert.assertEquals(new GtidSet(gtid).toString(), expected.toString());

        gtid = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:2";
        expected.expandTo(gtid);
        Assert.assertEquals(new GtidSet("b207f82e-2a7b-11ec-b128-1c34da51a830:25326877445,56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:2").toString(), expected.toString());
    }

    @Test
    public void testGetBriefGtidSet() {
        GtidSet gtidSet = new GtidSet("c4fff537-2a2a-11eb-aae0-506b4b4791b4:1-39:100-200,34b4ecc5-3675-11ea-a598-b8599ffdbbb4:1-362422");
        GtidSet briefGtidSet = gtidSet.getGtidFirstInterval();
        Assert.assertEquals(new GtidSet("c4fff537-2a2a-11eb-aae0-506b4b4791b4:1-39,34b4ecc5-3675-11ea-a598-b8599ffdbbb4:1-362422"), briefGtidSet);
    }

    @Test
    public void testGetGtidNum() {
        GtidSet gtidSet = new GtidSet("c4fff537-2a2a-11eb-aae0-506b4b4791b4:1-39:100-200:202,34b4ecc5-3675-11ea-a598-b8599ffdbbb4:1-362422");
        long num = gtidSet.getGtidNum();
        Assert.assertEquals(362563, num);
    }

    @Test
    public void testFindFirstGap() {
        GtidSet gap1 = new GtidSet("2764f97a-7ee6-11ee-8eea-fa163e991b93:2-100");
        GtidSet gap2 = new GtidSet("2764f97a-7ee6-11ee-8eea-fa163e991b93:1-100:102-105");
        GtidSet gap3 = new GtidSet("2764f97a-7ee6-11ee-8eea-fa163e991b93:2-100:102-105,c4fff537-2a2a-11eb-aae0-506b4b4791b4:1-39:100-200");
        GtidSet noGap = new GtidSet("2764f97a-7ee6-11ee-8eea-fa163e991b93:1-1005");
        Assert.assertEquals(1, gap1.findFirstGap().getUUIDSets().size());
        Assert.assertEquals("2764f97a-7ee6-11ee-8eea-fa163e991b93:1", gap1.findFirstGap().toString());
        
        Assert.assertEquals(1, gap2.findFirstGap().getUUIDSets().size());
        Assert.assertEquals("2764f97a-7ee6-11ee-8eea-fa163e991b93:101", gap2.findFirstGap().toString());

        Assert.assertEquals(2, gap3.findFirstGap().getUUIDSets().size());
        Assert.assertEquals("2764f97a-7ee6-11ee-8eea-fa163e991b93:1,c4fff537-2a2a-11eb-aae0-506b4b4791b4:40-99", gap3.findFirstGap().toString());

        Assert.assertEquals(0, noGap.findFirstGap().getUUIDSets().size());
        Assert.assertEquals("", noGap.findFirstGap().toString());
    }
}
