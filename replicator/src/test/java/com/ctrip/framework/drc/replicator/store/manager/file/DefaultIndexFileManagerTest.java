package com.ctrip.framework.drc.replicator.store.manager.file;

import com.ctrip.framework.drc.core.driver.binlog.constant.QueryType;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidManager;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcIndexLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.ITransactionEvent;
import com.ctrip.framework.drc.core.driver.binlog.manager.ApplyResult;
import com.ctrip.framework.drc.core.driver.binlog.manager.SchemaManager;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.core.server.utils.FileUtil;
import com.ctrip.framework.drc.replicator.container.zookeeper.UuidConfig;
import com.ctrip.framework.drc.replicator.container.zookeeper.UuidOperator;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.EventTransactionCache;
import com.ctrip.framework.drc.replicator.store.AbstractTransactionTest;
import com.ctrip.framework.drc.replicator.store.FilePersistenceEventStore;
import com.ctrip.framework.drc.replicator.store.manager.gtid.DefaultGtidManager;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.ctrip.framework.drc.core.driver.util.ByteHelper.FORMAT_LOG_EVENT_SIZE;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.EMPTY_DRC_UUID_EVENT_SIZE;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.EMPTY_PREVIOUS_GTID_EVENT_SIZE;
import static com.ctrip.framework.drc.replicator.AllTests.previous_gtidset_interval;
import static com.ctrip.framework.drc.replicator.store.manager.file.DefaultFileManager.LOG_EVENT_START;

/**
 * @Author limingdong
 * @create 2020/9/3
 */
public class DefaultIndexFileManagerTest extends AbstractTransactionTest {

    private GtidManager gtidManager;

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
        System.setProperty(SystemConfig.REPLICATOR_WHITE_LIST, String.valueOf(true));
        super.initMocks();
        when(replicatorConfig.getWhiteUUID()).thenReturn(uuids);
        when(replicatorConfig.getRegistryKey()).thenReturn("");
        when(uuidOperator.getUuids(anyString())).thenReturn(uuidConfig);
        when(uuidConfig.getUuids()).thenReturn(Sets.newHashSet());
        when(schemaManager.apply(anyString(), anyString(), anyString(), any(QueryType.class), anyString())).thenReturn(ApplyResult.from(ApplyResult.Status.SUCCESS, ""));
    }

    private void init() throws Exception {
        fileManager = new DefaultFileManager(schemaManager, DESTINATION);
        gtidManager = new DefaultGtidManager(fileManager, uuidOperator, replicatorConfig);
        fileManager.initialize();
        fileManager.start();
        fileManager.setGtidManager(gtidManager);

        ioCache = new FilePersistenceEventStore(fileManager, gtidManager);
        ioCache.initialize();
        ioCache.start();

        transactionCache = new EventTransactionCache(ioCache, filterChain, "ut_test");
        transactionCache.initialize();
        transactionCache.start();
    }

    @Test
    public void testIndexFile() throws Exception {
        init();
        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);
        int size = writeTransaction();
        int loop = previous_gtidset_interval / size;
        for (int i = 0; i < loop; ++i) {
            writeTransaction();
        }
        writeTransaction();

        List<File> files = FileUtil.sortDataDir(logDir.listFiles(), DefaultFileManager.LOG_FILE_PREFIX, false);
        int total = 0;
        for (File file : files) {
            total += file.length();
        }
        fileManager.flush();
        long actual = (LOG_EVENT_START + EMPTY_PREVIOUS_GTID_EVENT_SIZE + EMPTY_SCHEMA_EVENT_SIZE + EMPTY_DRC_UUID_EVENT_SIZE + FORMAT_LOG_EVENT_SIZE + DrcIndexLogEvent.FIX_SIZE + EMPTY_PREVIOUS_GTID_EVENT_SIZE) * files.size() + (loop + 2) * size;
        if (actual != total) {
            Assert.assertEquals((total - actual) % 40, 0);  //EMPTY_PREVIOUS_GTID_EVENT_SIZE not empty, 31 -> 71
        } else {
            Assert.assertEquals(total, actual);
        }
        GtidSet gtidSet = fileManager.getExecutedGtids();
        Assert.assertTrue(!gtidSet.getUUIDs().isEmpty());
        deleteFiles(logDir);
    }

    @Test
    public void testIndexFileWithDdl() throws Exception {
        init();
        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);
        int size = writeDdlTransaction();
        int loop = previous_gtidset_interval / size;
        for (int i = 0; i < loop; ++i) {
            writeDdlTransaction();
        }
        writeDdlTransaction();

        List<File> files = FileUtil.sortDataDir(logDir.listFiles(), DefaultFileManager.LOG_FILE_PREFIX, false);
        int total = 0;
        for (File file : files) {
            total += file.length();
        }
        fileManager.flush();
        Assert.assertEquals(total, (LOG_EVENT_START + EMPTY_PREVIOUS_GTID_EVENT_SIZE + EMPTY_SCHEMA_EVENT_SIZE + EMPTY_DRC_UUID_EVENT_SIZE + FORMAT_LOG_EVENT_SIZE + DrcIndexLogEvent.FIX_SIZE + EMPTY_PREVIOUS_GTID_EVENT_SIZE) * files.size() + (loop + 2) * size);
        GtidSet gtidSet = fileManager.getExecutedGtids();
        Assert.assertTrue(!gtidSet.getUUIDs().isEmpty());
        deleteFiles(logDir);
    }

    @Test
    public void testIndexFileWithDiffGtidSet() throws Exception {
        init();
        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);
        int size = writeTransactionWithGtid(UUID_STRING + ":" + 1);
        gtidManager.addExecutedGtid(UUID_STRING + ":" + 1);
        int loop = previous_gtidset_interval / size;
        for (int i = 2; i <= loop; ++i) {
            writeTransactionWithGtid(UUID_STRING + ":" + i);
            gtidManager.addExecutedGtid(UUID_STRING + ":" + i);
        }
        writeTransactionWithGtid(UUID_STRING + ":" + (loop + 1));
        gtidManager.addExecutedGtid(UUID_STRING + ":" + (loop + 1));

        List<File> files = FileUtil.sortDataDir(logDir.listFiles(), DefaultFileManager.LOG_FILE_PREFIX, false);
        int total = 0;
        for (File file : files) {
            total += file.length();
        }
        fileManager.flush();
        Assert.assertEquals(total, (LOG_EVENT_START + EMPTY_PREVIOUS_GTID_EVENT_SIZE + EMPTY_SCHEMA_EVENT_SIZE + EMPTY_DRC_UUID_EVENT_SIZE + FORMAT_LOG_EVENT_SIZE + DrcIndexLogEvent.FIX_SIZE + EMPTY_PREVIOUS_GTID_EVENT_SIZE + 40 /*not empty*/) * files.size() + (loop + 1) * size);
        GtidSet gtidSet = fileManager.getExecutedGtids();
        Assert.assertTrue(!gtidSet.getUUIDs().isEmpty());
        Assert.assertEquals(gtidSet.toString(), UUID_STRING + ":1-" + (loop + 1));
        deleteFiles(logDir);
    }

    @Test
    public void testIndexFileWithDiffGtidSetAndDdl() throws Exception {
        init();
        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);
        int size = writeTransactionWithGtid(UUID_STRING + ":" + 1);
        int ddlSize = 0;
        gtidManager.addExecutedGtid(UUID_STRING + ":" + 1);
        int loop = previous_gtidset_interval / size;
        for (int i = 2; i <= loop; ++i) {
            if (i == loop / 2) {
                int ddlTransactionSize = writeTransactionWithGtidAndDdl(UUID_STRING + ":" + i);
                ddlSize += ddlTransactionSize;
            } else {
                writeTransactionWithGtid(UUID_STRING + ":" + i);
            }
            gtidManager.addExecutedGtid(UUID_STRING + ":" + i);
        }
        writeTransactionWithGtid(UUID_STRING + ":" + (loop + 1));
        gtidManager.addExecutedGtid(UUID_STRING + ":" + (loop + 1));

        List<File> files = FileUtil.sortDataDir(logDir.listFiles(), DefaultFileManager.LOG_FILE_PREFIX, false);
        int total = 0;
        for (File file : files) {
            total += file.length();
        }
        fileManager.flush();
        Assert.assertEquals(total, (LOG_EVENT_START + EMPTY_PREVIOUS_GTID_EVENT_SIZE + EMPTY_SCHEMA_EVENT_SIZE + EMPTY_DRC_UUID_EVENT_SIZE + FORMAT_LOG_EVENT_SIZE + DrcIndexLogEvent.FIX_SIZE + EMPTY_PREVIOUS_GTID_EVENT_SIZE + 40 /*not empty*/) * files.size() + loop * size + ddlSize);
        GtidSet gtidSet = fileManager.getExecutedGtids();
        Assert.assertTrue(!gtidSet.getUUIDs().isEmpty());
        Assert.assertEquals(gtidSet.toString(), UUID_STRING + ":1-" + (loop + 1));
        deleteFiles(logDir);
    }

    @After
    public void tearDown() throws Exception {
        System.setProperty(SystemConfig.REPLICATOR_WHITE_LIST, String.valueOf(false));
        fileManager.stop();
        fileManager.dispose();
        fileManager.destroy();
    }
}
