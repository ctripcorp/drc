package com.ctrip.framework.drc.replicator.impl.oubound.handler;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.command.packet.applier.ApplierDumpCommandPacket;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.binlog.LocalBinlogScannerManager;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.binlog.LocalBinlogSender;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @author yongnian
 */
public class LocalBinlogScannerManagerTest {

    @Before
    public void setUp() {
//        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testTryMerge() throws Exception {
        LocalBinlogScannerManager localBinlogScannerManager = new LocalBinlogScannerManager();
        List<LocalBinlogSender> list = Lists.newArrayList();

        list.add((LocalBinlogSender) localBinlogScannerManager.startSender(null, new ApplierDumpCommandPacket("applier", new GtidSet("zyn:1-100,extra:1-10"))));
        list.add((LocalBinlogSender) localBinlogScannerManager.startSender(null, new ApplierDumpCommandPacket("applier", new GtidSet("zyn:1-105,extra:1-20"))));
        list.add((LocalBinlogSender) localBinlogScannerManager.startSender(null, new ApplierDumpCommandPacket("applier", new GtidSet("zyn:1-95,extra:1-20"))));
        list.add((LocalBinlogSender) localBinlogScannerManager.startSender(null, new ApplierDumpCommandPacket("applier", new GtidSet("zyn:1-150,extra:1-20"))));
        list.add((LocalBinlogSender) localBinlogScannerManager.startSender(null, new ApplierDumpCommandPacket("applier", new GtidSet("zyn:1-85,extra:1-20"))));
        list.add((LocalBinlogSender) localBinlogScannerManager.startSender(null, new ApplierDumpCommandPacket("applier", new GtidSet("zyn:1-20,extra:1-20"))));
        list.add((LocalBinlogSender) localBinlogScannerManager.startSender(null, new ApplierDumpCommandPacket("applier", new GtidSet("zyn:1-100,extra:1-20"))));
        list.add((LocalBinlogSender) localBinlogScannerManager.startSender(null, new ApplierDumpCommandPacket("applier", new GtidSet("zyn:1-115,extra:1-20"))));
        list.add((LocalBinlogSender) localBinlogScannerManager.startSender(null, new ApplierDumpCommandPacket("applier", new GtidSet("zyn:1-200,extra:1-20"))));
        list.add((LocalBinlogSender) localBinlogScannerManager.startSender(null, new ApplierDumpCommandPacket("applier", new GtidSet("zyn:1-185,extra:1-20"))));
        list.add((LocalBinlogSender) localBinlogScannerManager.startSender(null, new ApplierDumpCommandPacket("applier", new GtidSet("zyn:1-135,extra:1-20"))));
        list.add((LocalBinlogSender) localBinlogScannerManager.startSender(null, new ApplierDumpCommandPacket("applier", new GtidSet("zyn:1-103,extra:1-20"))));
        System.out.println("end");
        Thread.currentThread().join(5000);

        System.out.println("===== result =====");
        List<LocalBinlogSender> neverSend = Lists.newArrayList();
        List<GtidSet> sentGtid = Lists.newArrayList();
        for (LocalBinlogSender sender : list) {
            LocalBinlogSender.Report report = sender.getReport();
            GtidSet gtidSet = new GtidSet(report.initGtid);
            List<String> repeatGtid = Lists.newArrayList();
            if (report.sendGtidRecord.size() == 0) {
                neverSend.add(sender);
            } else {
                for (String s : report.sendGtidRecord) {
                    GtidSet gtid = new GtidSet(s);
                    if (gtid.isContainedWithin(gtidSet)) {
                        repeatGtid.add(s);
                        continue;
                    }
                    gtidSet.add(s);
                }
                Assert.assertEquals("[repeated] " + sender + ":" + repeatGtid, 0, repeatGtid.size());
                GtidSet gtidFirstInterval = gtidSet.getGtidFirstInterval();
                Assert.assertEquals("[gap] " + sender + ":" + gtidSet, gtidFirstInterval, gtidSet);
                sentGtid.add(gtidSet);
            }
        }

        GtidSet union = new GtidSet("");
        for (GtidSet gtidSet : sentGtid) {
            union = union.union(gtidSet);
        }

        for (LocalBinlogSender sender : neverSend) {
            Assert.assertFalse(sender + " never send !", new GtidSet(sender.report.initGtid).isContainedWithin(union));
        }


        GtidSet minSet = GtidSet.getIntersection(sentGtid);
        long gtidNum = union.subtract(minSet).getGtidNum();
        System.out.println("gtidNum = " + gtidNum);
//        Assert.assertTrue(String.valueOf(gtidNum), gtidNum <= 1);
    }
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme