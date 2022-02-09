package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.fetcher.resource.context.NetworkContextResource;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by jixinwang on 2022/2/9
 */
public class TransactionTableApplierDumpEventActivityTest {

    @Test
    public void updateGtidSet() throws Exception {
        TransactionTableApplierDumpEventActivity dumpEventActivity = new TransactionTableApplierDumpEventActivity();
        NetworkContextResource networkContextResource = new NetworkContextResource();
        networkContextResource.initialize();
        dumpEventActivity.setContext(networkContextResource);
        dumpEventActivity.updateGtidSet("45c8023b-888d-11ec-9f82-b8cef68a4636:4733");
        GtidSet gtidSet = networkContextResource.fetchGtidSet();
        Assert.assertEquals("45c8023b-888d-11ec-9f82-b8cef68a4636:4733", gtidSet.toString());
    }
}
