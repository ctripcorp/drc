package com.ctrip.framework.drc.fetcher.resource.position;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by dengquanliang
 * 2025/1/16 16:49
 */
public class MessengerGtidQueryTaskTest {

    public static Endpoint wrongCiEndpoint = new DefaultEndPoint("127.0.0.1", 3306, "root", "root");

    @Test
    public void doQuery() {
        MessengerGtidQueryTask messengerGtidQueryTask = new MessengerGtidQueryTask(wrongCiEndpoint, "registryKey");
        Pair<String, Boolean> res = messengerGtidQueryTask.getExecutedGtid();
        Assert.assertEquals("", res.getLeft());
        Assert.assertFalse(res.getRight());
    }
}
