package com.ctrip.framework.drc.core.driver.binlog.gtid;

import org.junit.Assert;
import org.junit.Test;

public class GtidTest {

    @Test
    public void testIsContainedWithin() throws Exception {
        Gtid gtid;
        gtid = new Gtid("abc:100");
        Assert.assertTrue(gtid.isContainedWithin(new GtidSet("abc:1-100")));
        Assert.assertTrue(gtid.isContainedWithin(new GtidSet("abc:1-101")));
        Assert.assertTrue(gtid.isContainedWithin(new GtidSet("abc:1-50:100")));
        Assert.assertTrue(gtid.isContainedWithin(new GtidSet("abc:1-50:100-101")));
        Assert.assertTrue(gtid.isContainedWithin(new GtidSet("abc:1-50:100-102")));
        Assert.assertTrue(gtid.isContainedWithin(new GtidSet("abc:1-50:99-100")));
        Assert.assertTrue(gtid.isContainedWithin(new GtidSet("abc:1-50:99-101")));
        Assert.assertTrue(gtid.isContainedWithin(new GtidSet("abc:1-50:99-102")));
        Assert.assertTrue(gtid.isContainedWithin(new GtidSet("abc:1-50:98-100")));
        Assert.assertTrue(gtid.isContainedWithin(new GtidSet("abc:1-50:98-101")));
        Assert.assertTrue(gtid.isContainedWithin(new GtidSet("abc:1-50:98-102")));

        Assert.assertFalse(gtid.isContainedWithin(new GtidSet("abc:1-98")));
        Assert.assertFalse(gtid.isContainedWithin(new GtidSet("abc:1-99")));
        Assert.assertFalse(gtid.isContainedWithin(new GtidSet("abc:1-98:150-151")));
        Assert.assertFalse(gtid.isContainedWithin(new GtidSet("abc:1-50:150-151")));
        Assert.assertFalse(gtid.isContainedWithin(new GtidSet("abc:1-50:97-99")));
        Assert.assertFalse(gtid.isContainedWithin(new GtidSet("abc:1-50:101-150")));
        Assert.assertFalse(gtid.isContainedWithin(new GtidSet("abc:1-50:97-99:101-150")));

        Assert.assertFalse(gtid.isContainedWithin(new GtidSet("xvf:1-100")));
        Assert.assertFalse(gtid.isContainedWithin(new GtidSet("")));
        Assert.assertFalse(gtid.isContainedWithin(null));
    }


    @Test
    public void testNew() {
        Gtid gtid1 = new Gtid("abc:123");
        Gtid gtid2 = new Gtid("abc", 123L);
        Assert.assertEquals(gtid2, gtid1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewFail() {
        Gtid gtid = new Gtid("abcde");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewFail2() {
        Gtid gtid = new Gtid("abcde:ab");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewFail3() {
        Gtid gtid = new Gtid("abcde:100-101");
    }


    @Test
    public void testToString() {
        String str = "abc:100";
        Gtid gtid = new Gtid(str);
        Assert.assertEquals(str, gtid.toString());
    }
}
