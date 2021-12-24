package com.ctrip.framework.drc.core.driver.command.netty;

import com.ctrip.framework.drc.core.AllTests;
import com.ctrip.framework.drc.core.MockTest;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.BorrowObjectException;
import com.ctrip.xpipe.pool.ReturnObjectException;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectState;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author limingdong
 * @create 2021/4/9
 */
public class DrcNettyClientPoolTest extends MockTest {

    @Mock
    private NettyClientFactory nettyClientFactory;

    @Mock
    private PooledObject<NettyClient> pooledObject;

    @Mock
    private NettyClient nettyClient;

    private DrcNettyClientPool nettyClientPool;

    private Endpoint endpoint = new DefaultEndPoint(AllTests.IP, AllTests.SRC_PORT, AllTests.MYSQL_USER, AllTests.MYSQL_PASSWORD);

    private AtomicInteger count = new AtomicInteger(0);

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    @Before
    public void setUp() throws Exception {
        super.initMocks();
        nettyClientPool = new DrcNettyClientPool(endpoint, nettyClientFactory);
        LifecycleHelper.initializeIfPossible(nettyClientPool);
        LifecycleHelper.startIfPossible(nettyClientPool);
    }

    @After
    public void tearDown() throws Exception {
        LifecycleHelper.stopIfPossible(nettyClientPool);
        LifecycleHelper.disposeIfPossible(nettyClientPool);
    }

    @Test
    public void testBorrowObject() throws BorrowObjectException, ReturnObjectException, InterruptedException {
        when(nettyClientFactory.makeObject()).thenReturn(pooledObject);
        when(pooledObject.getObject()).thenReturn(nettyClient);
        when(pooledObject.allocate()).thenReturn(true);
        when(pooledObject.deallocate()).thenReturn(true);
        when(nettyClientFactory.validateObject(pooledObject)).thenReturn(true);
        when(pooledObject.getState()).thenReturn(PooledObjectState.ALLOCATED);

        NettyClient tmpNettyClient = nettyClientPool.borrowObject();
        Assert.assertEquals(nettyClient, tmpNettyClient);

        new Thread(() -> {
            try {
                logger.info("[Thread] blocks on borrowObject");
                NettyClient nettyClient = nettyClientPool.borrowObject();  //test block on borrowObject, because of pool size is 1
                logger.info("[Thread] passes on borrowObject");
                Assert.assertEquals(count.get(), 1);
                nettyClientPool.returnObject(nettyClient);
            } catch (Exception e) {
                logger.error("nettyClientPool error", e);
            } finally {
                countDownLatch.countDown();
            }
        }).start();

        TimeUnit.MILLISECONDS.sleep(50);
        count.incrementAndGet();
        nettyClientPool.returnObject(tmpNettyClient);
        countDownLatch.await();

        nettyClientPool.clear();
    }
}