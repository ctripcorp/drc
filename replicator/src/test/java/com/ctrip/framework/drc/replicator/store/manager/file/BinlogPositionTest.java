package com.ctrip.framework.drc.replicator.store.manager.file;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author yongnian
 * @create 2024/9/14 15:42
 */
public class BinlogPositionTest {
    @Test
    public void testCompare() {
        // equals
        Assert.assertEquals(0, BinlogPosition.from(1, 123).compareTo(BinlogPosition.from(1, 123)));
        Assert.assertEquals(0, BinlogPosition.from(0, 0).compareTo(BinlogPosition.from(0, 0)));

        // bigger
        // different seq, compare seq
        Assert.assertTrue(BinlogPosition.from(2, 123).compareTo(BinlogPosition.from(1, 1)) > 0);
        Assert.assertTrue(BinlogPosition.from(2, 123).compareTo(BinlogPosition.from(1, 124)) > 0);
        Assert.assertTrue(BinlogPosition.from(2, 123).compareTo(BinlogPosition.from(1, 123)) > 0);

        // same seq, compare pos
        Assert.assertTrue(BinlogPosition.from(1, 123).compareTo(BinlogPosition.from(1, 120)) > 0);
        Assert.assertTrue(BinlogPosition.from(1, 123).compareTo(BinlogPosition.from(1, 1)) > 0);

        // smaller
        // different seq, compare seq
        Assert.assertTrue(BinlogPosition.from(1, 1).compareTo(BinlogPosition.from(2, 123)) < 0);
        Assert.assertTrue(BinlogPosition.from(1, 124).compareTo(BinlogPosition.from(2, 123)) < 0);
        Assert.assertTrue(BinlogPosition.from(1, 123).compareTo(BinlogPosition.from(2, 123)) < 0);

        // same seq, compare pos
        Assert.assertTrue(BinlogPosition.from(1, 120).compareTo(BinlogPosition.from(1, 123)) < 0);
        Assert.assertTrue(BinlogPosition.from(1, 1).compareTo(BinlogPosition.from(1, 123)) < 0);
    }

    @Test
    public void testGap() {
        Assert.assertEquals(122, BinlogPosition.from(1, 1).getGap(BinlogPosition.from(1, 123)));
        Assert.assertEquals(122, BinlogPosition.from(1, 123).getGap(BinlogPosition.from(1, 1)));

        Assert.assertEquals(122, BinlogPosition.from(1, 1).getGap(BinlogPosition.from(1, 123)));
        Assert.assertEquals(Integer.MAX_VALUE, BinlogPosition.from(2, 123).getGap(BinlogPosition.from(1, 1)));
    }

    @Test
    public void testMoveForward() {
        BinlogPosition binlogPosition = BinlogPosition.from(1, 1);

        BinlogPosition next = BinlogPosition.from(1, 123);
        Assert.assertTrue(binlogPosition.tryMoveForward(next));
        Assert.assertEquals(binlogPosition, next);
        Assert.assertNotSame(binlogPosition, next);

        next = BinlogPosition.from(2, 123);
        Assert.assertTrue(binlogPosition.tryMoveForward(next));
        Assert.assertEquals(binlogPosition, next);
        Assert.assertNotSame(binlogPosition, next);

        next = BinlogPosition.from(1, 123);
        Assert.assertFalse(binlogPosition.tryMoveForward(next));
        Assert.assertEquals(binlogPosition, BinlogPosition.from(2, 123));
        Assert.assertNotSame(binlogPosition, next);

        next = BinlogPosition.from(2, 100);
        Assert.assertFalse(binlogPosition.tryMoveForward(next));
        Assert.assertEquals(binlogPosition, BinlogPosition.from(2, 123));
        Assert.assertNotSame(binlogPosition, next);

        next = BinlogPosition.from(2, 123);
        Assert.assertFalse(binlogPosition.tryMoveForward(next));
        Assert.assertEquals(binlogPosition, BinlogPosition.from(2, 123));
        Assert.assertNotSame(binlogPosition, next);
    }
}
