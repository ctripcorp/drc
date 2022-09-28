package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import org.junit.Assert;
import org.junit.Test;

import static com.ctrip.framework.drc.replicator.impl.inbound.filter.TransactionFlags.*;

/**
 * @Author limingdong
 * @create 2022/9/28
 */
public class TransactionFlagsTest {

    @Test
    public void testTransactionFlags() {
        TransactionFlags flags = new TransactionFlags();

        flags.mark(GTID_F);
        Assert.assertTrue(flags.filtered());
        Assert.assertTrue(flags.gtidFiltered());
        Assert.assertFalse(flags.transactionTableFiltered());
        Assert.assertFalse(flags.blackTableFiltered());
        Assert.assertFalse(flags.otherFiltered());
        flags.unmark(GTID_F);

        Assert.assertFalse(flags.filtered());
        Assert.assertFalse(flags.gtidFiltered());
        Assert.assertFalse(flags.transactionTableFiltered());
        Assert.assertFalse(flags.blackTableFiltered());
        Assert.assertFalse(flags.otherFiltered());

        flags.mark(BLACK_TABLE_NAME_F);
        Assert.assertTrue(flags.filtered());
        Assert.assertTrue(flags.blackTableFiltered());
        Assert.assertFalse(flags.gtidFiltered());
        Assert.assertFalse(flags.transactionTableFiltered());
        Assert.assertFalse(flags.otherFiltered());
        flags.unmark(BLACK_TABLE_NAME_F);

        flags.mark(TRANSACTION_TABLE_F);
        Assert.assertTrue(flags.filtered());
        Assert.assertTrue(flags.transactionTableFiltered());
        Assert.assertFalse(flags.blackTableFiltered());
        Assert.assertFalse(flags.gtidFiltered());
        Assert.assertFalse(flags.otherFiltered());
        flags.reset();

        flags.mark(OTHER_F);
        Assert.assertTrue(flags.filtered());
        Assert.assertTrue(flags.otherFiltered());
        Assert.assertFalse(flags.transactionTableFiltered());
        Assert.assertFalse(flags.blackTableFiltered());
        Assert.assertFalse(flags.gtidFiltered());
        flags.unmark(OTHER_F);

        Assert.assertFalse(flags.filtered());
        Assert.assertFalse(flags.gtidFiltered());
        Assert.assertFalse(flags.transactionTableFiltered());
        Assert.assertFalse(flags.blackTableFiltered());
        Assert.assertFalse(flags.otherFiltered());

        flags.mark(GTID_F);
        flags.mark(BLACK_TABLE_NAME_F);
        flags.mark(TRANSACTION_TABLE_F);
        flags.mark(OTHER_F);
        Assert.assertTrue(flags.filtered());
        Assert.assertTrue(flags.gtidFiltered());
        Assert.assertTrue(flags.transactionTableFiltered());
        Assert.assertTrue(flags.blackTableFiltered());
        Assert.assertTrue(flags.otherFiltered());

        flags.reset();

        Assert.assertFalse(flags.filtered());
        Assert.assertFalse(flags.gtidFiltered());
        Assert.assertFalse(flags.transactionTableFiltered());
        Assert.assertFalse(flags.blackTableFiltered());
        Assert.assertFalse(flags.otherFiltered());
    }
}