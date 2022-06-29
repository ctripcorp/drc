package com.ctrip.framework.drc.replicator.store;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.impl.ITransactionEvent;
import com.ctrip.framework.drc.core.driver.binlog.manager.SchemaManager;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.replicator.container.zookeeper.UuidConfig;
import com.ctrip.framework.drc.replicator.container.zookeeper.UuidOperator;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.EventTransactionCache;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Set;
import java.util.UUID;

/**
 * Created by mingdongli
 * 2019/9/18 下午2:35.
 */
public class FilePersistenceEventStoreTest extends AbstractTransactionTest{

    private FilePersistenceEventStore eventStore;

    @Mock
    private SchemaManager schemaManager;

    @Mock
    private Filter<ITransactionEvent> filterChain;

    @Mock
    private ReplicatorConfig replicatorConfig;

    @Mock
    private UuidOperator uuidOperator;

    @Mock
    private UuidConfig uuidConfig;

    private Set<UUID> uuids = Sets.newHashSet();

    @Before
    public void setUp() throws Exception {
        System.setProperty(SystemConfig.REPLICATOR_WHITE_LIST, String.valueOf(true));
        super.initMocks();
        when(replicatorConfig.getWhiteUUID()).thenReturn(uuids);
        when(replicatorConfig.getRegistryKey()).thenReturn("");
        when(uuidOperator.getUuids(anyString())).thenReturn(uuidConfig);
        when(uuidConfig.getUuids()).thenReturn(Sets.newHashSet("c372080a-1804-11ea-8add-98039bbedf9c"));

        System.setProperty(SystemConfig.REPLICATOR_FILE_LIMIT, String.valueOf(1024 * 2));
        eventStore = new FilePersistenceEventStore(schemaManager, uuidOperator, replicatorConfig);
        fileManager = eventStore.getFileManager();

        eventStore.initialize();
        eventStore.start();

        transactionCache = new EventTransactionCache(eventStore, filterChain);
        transactionCache.initialize();
        transactionCache.start();
    }

    @After
    public void tearDown() throws Exception {
        System.setProperty(SystemConfig.REPLICATOR_WHITE_LIST, String.valueOf(false));
        eventStore.stop();
        eventStore.dispose();
    }

    @Test
    public void testWriteByteBuf() throws Exception {
        writeTransactionThroughTransactionCache();
        GtidSet gtidSet = fileManager.getExecutedGtids();
        Assert.assertEquals(gtidSet.add(GTID), false);
    }

}
