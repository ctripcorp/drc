package com.ctrip.framework.drc.manager.healthcheck.tracker;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.server.utils.IpUtils;
import com.ctrip.framework.drc.manager.healthcheck.HeartBeatContext;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.ctrip.framework.drc.manager.AllTests.*;

/**
 * Created by mingdongli
 * 2019/11/28 下午2:05.
 */
public class HeartBeatTrackerImplTest implements HeartBeatTracker.HeartbeatExpirer {

    private static final String IP = IpUtils.getFistNonLocalIpv4ServerAddress();

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    private HeartBeatTrackerImpl heartBeatTracker;

    private HeartBeatContext heartBeatContext;

    private Endpoint endpoint;

    @Before
    public void setUp() throws Exception {
        endpoint = new DefaultEndPoint(MYSQL_IP, SRC_PORT, MYSQL_USER, MYSQL_PASSWORD);

        heartBeatContext = new HeartBeatContext() {
            @Override
            public boolean accept(Endpoint ip) {
                return true;
            }

            @Override
            public int getLease() {
                return 300;
            }

            @Override
            public int getInterval() {
                return 100;
            }
        };
        heartBeatTracker = new HeartBeatTrackerImpl(heartBeatContext);
        heartBeatTracker.setExpirer(this);
        heartBeatTracker.initialize();
        heartBeatTracker.start();
    }

    @After
    public void tearDown() throws Exception {
        heartBeatTracker.stop();
        heartBeatTracker.dispose();
    }

    @Test
    public void testAddHeartbeat() throws InterruptedException {
        heartBeatTracker.addHeartbeat(endpoint, heartBeatContext.getLease());
        countDownLatch.await();
        Assert.assertFalse(heartBeatTracker.hasHeartbeat(endpoint));
    }

    @Override
    public void expire(List<Endpoint> downs) {
        Assert.assertEquals(downs.size(), 1);
        Assert.assertEquals(downs.get(0), endpoint);
        countDownLatch.countDown();
    }

    @Override
    public String getServerId() {
        return IP;
    }
}