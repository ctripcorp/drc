package com.ctrip.framework.drc.replicator.impl.oubound.binlog;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.command.packet.applier.ApplierDumpCommandPacket;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.binlog.LocalBinlogScanner;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.binlog.LocalBinlogScannerManager;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.binlog.LocalBinlogSender;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yongnian
 */
public class BinlogScannerMergeAlgorithmTest {

    @Test
    public void test() {
        List<BinlogScanner> binlogScanners;
        AbstractBinlogScannerManager.MergeAlgorithm calculate;
        BinlogScanner expect;
        // 1. can merge
        expect = buildScanner(4, new GtidSet("zyn:1-20"));
        binlogScanners = Lists.newArrayList(
                expect,
                buildScanner(2, new GtidSet("zyn:1-10")),
                buildScanner(3, new GtidSet("zyn:1-20"))
        );
        calculate = AbstractBinlogScannerManager.MergeAlgorithm.calculate(binlogScanners, 100, 100);
        Assert.assertTrue(calculate.canMerge());
        Assert.assertEquals(1, calculate.candidates.size());
        Assert.assertEquals(expect, calculate.mergeTo);

        expect = buildScanner(3, new GtidSet("zyn:1-20"));
        binlogScanners = Lists.newArrayList(
                expect,
                buildScanner(2, new GtidSet("zyn:1-20")),
                buildScanner(1, new GtidSet("zyn:1-20"))
        );
        calculate = AbstractBinlogScannerManager.MergeAlgorithm.calculate(binlogScanners, 100, 100);
        Assert.assertTrue(calculate.canMerge());
        Assert.assertEquals(2, calculate.candidates.size());
        Assert.assertEquals(expect, calculate.mergeTo);


        expect = buildScanner(2, new GtidSet("zyn:1-10"));
        binlogScanners = Lists.newArrayList(
                expect,
                buildScanner(2, new GtidSet("zyn:1-20")),
                buildScanner(3, new GtidSet("zyn:1-20"))
        );
        calculate = AbstractBinlogScannerManager.MergeAlgorithm.calculate(binlogScanners, 100, 100);
        Assert.assertTrue(calculate.canMerge());
        Assert.assertEquals(1, calculate.candidates.size());
        Assert.assertEquals(expect, calculate.mergeTo);


        // test max sender number
        expect = buildScanner(3, new GtidSet("zyn:1-10"));
        binlogScanners = Lists.newArrayList(
                expect,
                buildScanner(2, new GtidSet("zyn:1-11")),
                buildScanner(1, new GtidSet("zyn:1-12")),
                buildScanner(1, new GtidSet("zyn:1-13"))
        );
        calculate = AbstractBinlogScannerManager.MergeAlgorithm.calculate(binlogScanners, 100, 6);
        Assert.assertTrue(calculate.canMerge());
        Assert.assertEquals(2, calculate.candidates.size());
        Assert.assertEquals(expect, calculate.mergeTo);


        expect = buildScanner(3, new GtidSet("zyn:1-10"));
        binlogScanners = Lists.newArrayList(
                expect,
                buildScanner(2, new GtidSet("zyn:1-20")),
                buildScanner(1, new GtidSet("zyn:1-20")),
                buildScanner(1, new GtidSet("zyn:1-20"))
        );
        calculate = AbstractBinlogScannerManager.MergeAlgorithm.calculate(binlogScanners, 100, 6);
        Assert.assertTrue(calculate.canMerge());
        Assert.assertEquals(2, calculate.candidates.size());
        Assert.assertEquals(expect, calculate.mergeTo);

        expect = buildScanner(3, new GtidSet("zyn:1-10"));
        binlogScanners = Lists.newArrayList(
                expect,
                buildScanner(2, new GtidSet("zyn:1-20")),
                buildScanner(1, new GtidSet("zyn:1-20")),
                buildScanner(1, new GtidSet("zyn:1-20"))
        );
        calculate = AbstractBinlogScannerManager.MergeAlgorithm.calculate(binlogScanners, 100, 7);
        Assert.assertTrue(calculate.canMerge());
        Assert.assertEquals(3, calculate.candidates.size());
        Assert.assertEquals(expect, calculate.mergeTo);


        // test max gtid gap
        expect = buildScanner(3, new GtidSet("zyn:1-10"));
        binlogScanners = Lists.newArrayList(
                expect,
                buildScanner(2, new GtidSet("zyn:1-110")),
                buildScanner(2, new GtidSet("zyn:1-111")),
                buildScanner(1, new GtidSet("zyn:1-20"))
        );
        calculate = AbstractBinlogScannerManager.MergeAlgorithm.calculate(binlogScanners, 100, 100);
        Assert.assertTrue(calculate.canMerge());
        Assert.assertEquals(2, calculate.candidates.size());
        Assert.assertEquals(expect, calculate.mergeTo);

        // max gtid gap && max sender num
        expect = buildScanner(5, new GtidSet("zyn:1-100"));
        binlogScanners = Lists.newArrayList(
                expect,
                buildScanner(7, new GtidSet("zyn:1-10")), // slowest, but cannot be mergeTo because gap && number
                buildScanner(7, new GtidSet("zyn:1-111")),
                buildScanner(2, new GtidSet("zyn:1-111"))
                );
        calculate = AbstractBinlogScannerManager.MergeAlgorithm.calculate(binlogScanners, 100, 10);
        Assert.assertTrue(calculate.canMerge());
        Assert.assertEquals(1, calculate.candidates.size());
        Assert.assertEquals(expect, calculate.mergeTo);


        // test no merge
        expect = buildScanner(10, new GtidSet("zyn:1-1000"));
        binlogScanners = Lists.newArrayList(
                expect,
                buildScanner(8, new GtidSet("zyn:1-110")),
                buildScanner(7, new GtidSet("zyn:1-111")),
                buildScanner(5, new GtidSet("zyn:1-20"))
        );
        calculate = AbstractBinlogScannerManager.MergeAlgorithm.calculate(binlogScanners, 100, 10);
        Assert.assertFalse(calculate.canMerge());


        // test merge order: by gtid gap
        expect = buildScanner(5, new GtidSet("zyn:1-10"));
        BinlogScanner candidate1 = buildScanner(2, new GtidSet("zyn:1-30"));
        BinlogScanner candidate2 = buildScanner(3, new GtidSet("zyn:1-40"));
        binlogScanners = Lists.newArrayList(
                expect,
                buildScanner(3, new GtidSet("zyn:1-50")),
                candidate1,
                candidate2
        );
        calculate = AbstractBinlogScannerManager.MergeAlgorithm.calculate(binlogScanners, 100, 10);
        Assert.assertTrue(calculate.canMerge());
        Assert.assertEquals(expect, calculate.mergeTo);
        Assert.assertEquals(2, calculate.candidates.size());
        Assert.assertTrue(calculate.candidates.contains(candidate1));
        Assert.assertTrue(calculate.candidates.contains(candidate2));

    }

    private BinlogScanner buildScanner(int size, GtidSet gtidSet) {
        List<BinlogSender> list = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            list.add(new LocalBinlogSender(null, new ApplierDumpCommandPacket(null, gtidSet)));
        }
        return new LocalBinlogScanner(new LocalBinlogScannerManager(), list);
    }
}