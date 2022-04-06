package com.ctrip.framework.drc.replicator.store.manager.gtid;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidManager;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.impl.GtidLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.manager.SchemaManager;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.replicator.container.zookeeper.UuidConfig;
import com.ctrip.framework.drc.replicator.container.zookeeper.UuidOperator;
import com.ctrip.framework.drc.replicator.store.AbstractTransactionTest;
import com.ctrip.framework.drc.replicator.store.manager.file.DefaultFileManager;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Author limingdong
 * @create 2019/12/31
 */
public class GtidConsumerTest extends AbstractTransactionTest {

    private GtidConsumer gtidConsumer = new GtidConsumer(true);

    private ExecutorService gtidService = Executors.newSingleThreadExecutor();

    @Mock
    private SchemaManager schemaManager;

    @Mock
    private ReplicatorConfig replicatorConfig;

    @Mock
    private UuidOperator uuidOperator;

    @Mock
    private UuidConfig uuidConfig;

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    private Set<UUID> uuids = Sets.newHashSet();

    @Before
    public void setUp() {
        super.initMocks();
        when(replicatorConfig.getWhiteUUID()).thenReturn(uuids);
        when(replicatorConfig.getRegistryKey()).thenReturn("");
        when(uuidOperator.getUuids(anyString())).thenReturn(uuidConfig);
        when(uuidConfig.getUuids()).thenReturn(Sets.newHashSet("c372080a-1804-11ea-8add-98039bbedf9c"));

    }

    @Test
    public void offer() throws InterruptedException {
        int SIZE = 100000;
        List<LogEvent> events = new ArrayList<>(SIZE);
        gtidService.submit(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < SIZE; ++i) {
                    ByteBuf byteBuf = getGtidEvent();
                    GtidLogEvent gtidLogEvent = new GtidLogEvent().read(byteBuf);
                    byteBuf.release();
                    gtidConsumer.offer(gtidLogEvent);
                    events.add(gtidLogEvent);
                }
                countDownLatch.countDown();
            }
        });

        countDownLatch.await();

        GtidSet gtidSet = gtidConsumer.getGtidSet();
        events.forEach(logEvent -> {
            try {
                logEvent.release();
            } catch (Exception e) {
            }
        });
        Assert.assertEquals(gtidSet.toString(), "a0a1fbb8-bdc8-11e9-96a0-fa163e7af2ad:66");

    }

    @Test
    public void testFileManager() throws Exception {
        fileManager = new DefaultFileManager(schemaManager, "consume");
        GtidManager gtidManager = new DefaultGtidManager(fileManager, uuidOperator, replicatorConfig);
        fileManager.initialize();
        fileManager.start();
        fileManager.setGtidManager(gtidManager);
        try {
            File logDir = fileManager.getDataDir();
            deleteFiles(logDir);
            writePreviousGtid();
            writeTransaction();

            GtidSet gtidSet = fileManager.getExecutedGtids();
            GtidSet previousGtidSet = new GtidSet(PREVIOUS_GTID);
            Assert.assertTrue(previousGtidSet.isContainedWithin(gtidSet));
        } catch (Exception e) {
            logger.error("getExecutedGtids error", e);
        }
    }

    @Test
    public void testFileManagerReality() throws Exception {
        fileManager = new DefaultFileManager(schemaManager,"unitTest");
        GtidManager gtidManager = new DefaultGtidManager(fileManager, uuidOperator, replicatorConfig);
        fileManager.initialize();
        fileManager.start();
        fileManager.setGtidManager(gtidManager);
        try {
            GtidSet gtidSet = fileManager.getExecutedGtids();
            logger.info("gtidSet is {}", gtidSet);
        } catch (Exception e) {
            logger.error("getExecutedGtids error", e);
        }
    }
}