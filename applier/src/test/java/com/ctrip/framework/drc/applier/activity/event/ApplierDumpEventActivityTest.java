package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.fetcher.event.ApplierGtidEvent;
import com.ctrip.framework.drc.fetcher.resource.context.NetworkContextResource;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by jixinwang on 2023/12/6
 */
public class ApplierDumpEventActivityTest {

    private ApplierDumpEventActivity dumpEventActivity;

    private String uuid = "45c8023b-888d-11ec-9f82-b8cef68a4636";

    @Before
    public void setUp() throws Exception {
        dumpEventActivity = new ApplierDumpEventActivity();
        NetworkContextResource networkContextResource = new NetworkContextResource();
        networkContextResource.initialize();
        dumpEventActivity.setContext(networkContextResource);
    }

    @Test
    public void checkPositionGap() {
        GtidSet gtidSet = new GtidSet(uuid + ":1-10:30:50-60");
        dumpEventActivity.updateContextGtidSet(gtidSet);

        ApplierGtidEvent applierGtidEvent1 = new ApplierGtidEvent(uuid + ":15");
        dumpEventActivity.compensateGapIfNeed(applierGtidEvent1, uuid);
        Assert.assertEquals(uuid + ":1-14", dumpEventActivity.toInitGap.toString());

        ApplierGtidEvent applierGtidEvent2 = new ApplierGtidEvent(uuid + ":20");
        dumpEventActivity.compensateGapIfNeed(applierGtidEvent2, uuid);
        Assert.assertEquals(uuid + ":1-14:16-19", dumpEventActivity.toInitGap.toString());

        ApplierGtidEvent applierGtidEvent3 = new ApplierGtidEvent(uuid + ":21");
        dumpEventActivity.compensateGapIfNeed(applierGtidEvent3, uuid);
        Assert.assertEquals(uuid + ":1-14:16-19", dumpEventActivity.toInitGap.toString());

        ApplierGtidEvent applierGtidEvent4 = new ApplierGtidEvent(uuid + ":61");
        dumpEventActivity.compensateGapIfNeed(applierGtidEvent4, uuid);
        Assert.assertEquals(StringUtils.EMPTY, dumpEventActivity.toInitGap.toString());
        Assert.assertEquals(uuid + ":1-14:16-19:22-60", dumpEventActivity.getContext().fetchGtidSet().toString());

        ApplierGtidEvent applierGtidEvent5 = new ApplierGtidEvent(uuid + ":65");
        dumpEventActivity.compensateGapIfNeed(applierGtidEvent5, uuid);
        Assert.assertEquals(uuid + ":1-14:16-19:22-60:62-64", dumpEventActivity.getContext().fetchGtidSet().toString());
    }
}