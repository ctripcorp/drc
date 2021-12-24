package com.ctrip.framework.drc.manager.healthcheck.notifier;

import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.manager.AllTests;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.retry.RestOperationsRetryPolicyFactory;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.ctrip.framework.drc.manager.AllTests.ID_PUT;
import static com.github.tomakehurst.wiremock.client.WireMock.*;

/**
 * @Author limingdong
 * @create 2020/3/4
 */
@FixMethodOrder(value = MethodSorters.NAME_ASCENDING)
public class ReplicatorNotifierTest extends AbstractNotifierTest {

    private ReplicatorNotifier replicatorNotifier;

    private static final int CONNECT_TIME_OUT = 300;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        replicatorNotifier = ReplicatorNotifier.getInstance();
        replicatorNotifier.setRestTemplate(RestTemplateFactory.createCommonsHttpRestTemplate(2, 20, CONNECT_TIME_OUT, 5000, 0, new RestOperationsRetryPolicyFactory(1)));
    }

    @Test
    public void test_01_notifyPut() {
        int previousRetryInterval = AbstractNotifier.RETRY_INTERVAL;
        AbstractNotifier.RETRY_INTERVAL = 10;
        replicatorNotifier.notifyAdd(dbCluster);
        //first request fail and success after retry
        AllTests.wireMockServer.editStub(put(urlPathMatching("/replicators")).withId(ID_PUT).willReturn(okJson(Codec.DEFAULT.encode(ApiResult.getSuccessInstance(true)))));
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));

        String message = baos.toString();
        Assert.assertTrue(message.contains("already run"));
        baos.reset();

        AbstractNotifier.RETRY_INTERVAL = previousRetryInterval;
    }

    @Test
    public void test_02_notifyDelete() throws IOException {
        resetOut();
        int previousRetryInterval = AbstractNotifier.RETRY_INTERVAL;
        AbstractNotifier.RETRY_INTERVAL = 10;
        replicatorNotifier.notifyRemove("other_name", dbCluster.getReplicators().get(0), true);
        //first request fail and success after retry
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));

        String message = baos.toString();
        Assert.assertTrue(!message.contains("already run"));
        baos.reset();

        AbstractNotifier.RETRY_INTERVAL = previousRetryInterval;

    }
}