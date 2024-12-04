package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.fetcher.event.ApplierDrcUuidLogEvent;
import com.ctrip.framework.drc.fetcher.event.ApplierPreviousGtidsLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.fetcher.event.ApplierGtidEvent;
import com.ctrip.framework.drc.fetcher.event.ApplierXidEvent;
import com.ctrip.framework.drc.fetcher.resource.context.NetworkContextResource;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.util.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by jixinwang on 2023/12/6
 */
public class ApplierDumpEventActivityTest {

    private ApplierDumpEventActivity dumpEventActivity;

    private String uuid1 = "45c8023b-888d-11ec-9f82-b8cef68a4636";
    private String uuid2 = "65c8023b-888d-11ec-9f82-b8cef68a4632";
    private String uuid3 = "95c8023b-888d-11ec-9f82-b8cef68a4631";

    @Before
    public void setUp() throws Exception {
        dumpEventActivity = new ApplierDumpEventActivity();
        NetworkContextResource networkContextResource = new NetworkContextResource();
        networkContextResource.initialize();
        dumpEventActivity.setContext(networkContextResource);
    }

    @Test
    public void testCompensateGap() {
        GtidSet receivedGtidSet = new GtidSet(uuid1 + ":1-10:30:50-60");
        dumpEventActivity.updateContextGtidSet(receivedGtidSet);

        ApplierGtidEvent gtid1 = new ApplierGtidEvent(uuid1 + ":15");
        dumpEventActivity.doHandleLogEvent(gtid1);
        Assert.assertEquals(uuid1 + ":2-14", dumpEventActivity.toInitGap.toString());

        ApplierGtidEvent gtid2 = new ApplierGtidEvent(uuid1 + ":20");
        dumpEventActivity.doHandleLogEvent(gtid2);
        Assert.assertEquals(uuid1 + ":2-14:16-19", dumpEventActivity.toInitGap.toString());

        ApplierGtidEvent gtid3 = new ApplierGtidEvent(uuid1 + ":22");
        dumpEventActivity.doHandleLogEvent(gtid3);
        Assert.assertEquals(uuid1 + ":2-14:16-19:21", dumpEventActivity.toInitGap.toString());
        Assert.assertEquals(uuid1 + ":1-10:30:50-60", dumpEventActivity.getContext().fetchGtidSet().toString());

        ApplierGtidEvent gtid4 = new ApplierGtidEvent(uuid1 + ":61");
        dumpEventActivity.doHandleLogEvent(gtid4);
        Assert.assertEquals(StringUtils.EMPTY, dumpEventActivity.toInitGap.toString());
        Assert.assertEquals(uuid1 + ":1-14:16-19:21:23-60", dumpEventActivity.getContext().fetchGtidSet().toString());

        ApplierGtidEvent gtid5 = new ApplierGtidEvent(uuid1 + ":65");
        dumpEventActivity.doHandleLogEvent(gtid5);
        Assert.assertEquals(uuid1 + ":1-14:16-19:21:23-60:62-64", dumpEventActivity.getContext().fetchGtidSet().toString());

        // uuid switch
        ApplierGtidEvent gtid10 = new ApplierGtidEvent(uuid2 + ":10");
        dumpEventActivity.doHandleLogEvent(gtid10);
        Assert.assertEquals(StringUtils.EMPTY, dumpEventActivity.toInitGap.toString());

        ApplierGtidEvent gtid11 = new ApplierGtidEvent(uuid2 + ":20");
        dumpEventActivity.doHandleLogEvent(gtid11);
        Assert.assertEquals(StringUtils.EMPTY, dumpEventActivity.toInitGap.toString());
        Assert.assertEquals(uuid1 + ":1-14:16-19:21:23-60:62-64," + uuid2 + ":11-19", dumpEventActivity.getContext().fetchGtidSet().toString());

        // mixed uuid
        ApplierGtidEvent gtid20 = new ApplierGtidEvent(uuid1 + ":90");
        dumpEventActivity.doHandleLogEvent(gtid20);
        ApplierGtidEvent gtid21 = new ApplierGtidEvent(uuid2 + ":30");
        dumpEventActivity.doHandleLogEvent(gtid21);
        ApplierGtidEvent gtid22 = new ApplierGtidEvent(uuid1 + ":100");
        dumpEventActivity.doHandleLogEvent(gtid22);
        ApplierGtidEvent gtid23 = new ApplierGtidEvent(uuid2 + ":50");
        dumpEventActivity.doHandleLogEvent(gtid23);
        ApplierGtidEvent gtid24 = new ApplierGtidEvent(uuid2 + ":60");
        dumpEventActivity.doHandleLogEvent(gtid24);
        Assert.assertEquals(uuid1 + ":1-14:16-19:21:23-60:62-64:66-89:91-99," + uuid2 + ":11-19:21-29:31-49:51-59",
                dumpEventActivity.getContext().fetchGtidSet().toString());
    }

    @Test
    public void testCompensateGapOfDiffUuid() {
        ApplierGtidEvent gtid1 = new ApplierGtidEvent(uuid2 + ":50");
        dumpEventActivity.doHandleLogEvent(gtid1);
        dumpEventActivity.doHandleLogEvent(new ApplierXidEvent());

        ApplierPreviousGtidsLogEvent previousGtidsLogEvent = new ApplierPreviousGtidsLogEvent();
        previousGtidsLogEvent.setGtidSet(new GtidSet(uuid1 + ":1-100," + uuid2 + ":1-200"));
        dumpEventActivity.doHandleLogEvent(previousGtidsLogEvent);

        ApplierDrcUuidLogEvent uuidLogEvent = new ApplierDrcUuidLogEvent();
        uuidLogEvent.setContent(Sets.newLinkedHashSet(uuid1, uuid2, uuid3));
        dumpEventActivity.doHandleLogEvent(uuidLogEvent);

        Assert.assertEquals(uuid2 + ":1-200," + uuid1 + ":1-100",
                dumpEventActivity.getContext().fetchGtidSet().toString());
    }
}