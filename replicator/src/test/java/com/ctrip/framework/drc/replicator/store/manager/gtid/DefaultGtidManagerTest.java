package com.ctrip.framework.drc.replicator.store.manager.gtid;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.impl.ITransactionEvent;
import com.ctrip.framework.drc.core.driver.binlog.manager.SchemaManager;
import com.ctrip.framework.drc.core.server.common.Filter;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.replicator.container.zookeeper.UuidConfig;
import com.ctrip.framework.drc.replicator.container.zookeeper.UuidOperator;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.EventTransactionCache;
import com.ctrip.framework.drc.replicator.store.AbstractTransactionTest;
import com.ctrip.framework.drc.replicator.store.FilePersistenceEventStore;
import com.ctrip.framework.drc.replicator.store.manager.file.DefaultFileManager;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.File;
import java.util.Set;
import java.util.UUID;

/**
 * Created by mingdongli
 * 2019/10/26 8:54.
 */
public class DefaultGtidManagerTest extends AbstractTransactionTest {

    private DefaultGtidManager gtidManager;

    @Mock
    private SchemaManager schemaManager;

    @Mock
    private ReplicatorConfig replicatorConfig;

    @Mock
    private UuidOperator uuidOperator;

    @Mock
    private UuidConfig uuidConfig;

    @Mock
    private Filter<ITransactionEvent> filterChain;

    private Set<UUID> uuids = Sets.newHashSet();

    @Before
    public void setUp() throws Exception {
        super.initMocks();
        when(replicatorConfig.getWhiteUUID()).thenReturn(uuids);
        when(replicatorConfig.getRegistryKey()).thenReturn("");
        when(uuidOperator.getUuids(anyString())).thenReturn(uuidConfig);
        when(uuidConfig.getUuids()).thenReturn(Sets.newHashSet("c372080a-1804-11ea-8add-98039bbedf9c"));

        System.setProperty(SystemConfig.REPLICATOR_FILE_LIMIT, String.valueOf(512 * 1000 * 1000));
        fileManager = new DefaultFileManager(schemaManager, DESTINATION);
        gtidManager = new DefaultGtidManager(fileManager, uuidOperator, replicatorConfig);
        fileManager.setGtidManager(gtidManager);
        gtidManager.initialize();
        gtidManager.start();
    }

    @After
    public void tearDown() throws Exception {
        gtidManager.stop();
        gtidManager.doDispose();
        fileManager.destroy();
    }

    @Test
    public void getExecutedGtids() throws Exception {
        writeTransaction();
        GtidSet gtidSet = gtidManager.getExecutedGtids();
        Assert.assertTrue(gtidSet.add(GTID));
        Assert.assertFalse(gtidSet.add(GTID));
    }

    @Test
    public void getPurgedGtids() throws Exception {
        writeTransaction();
        GtidSet gtidSet = gtidManager.getPurgedGtids();
        Assert.assertTrue(gtidSet.isContainedWithin(new GtidSet("")));
    }

    @Test
    public void testGrandTransaction() throws Exception {
        ioCache = new FilePersistenceEventStore(fileManager, gtidManager);
        ioCache.initialize();
        ioCache.start();

        transactionCache = new EventTransactionCache(ioCache, filterChain);
        transactionCache.initialize();
        transactionCache.start();
        transactionCache.addObserver(gtidManager);  // transactionCache notify gtid to gtidmanager

        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);
        writeGrandTransaction();
        GtidSet gtidSet = fileManager.getExecutedGtids();
        Assert.assertEquals(gtidSet.add(GTID), false);
    }
}
