package com.ctrip.framework.drc.replicator.store.manager.file;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidManager;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.impl.*;
import com.ctrip.framework.drc.core.driver.binlog.manager.SchemaManager;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.core.server.utils.FileUtil;
import com.ctrip.framework.drc.replicator.container.zookeeper.UuidConfig;
import com.ctrip.framework.drc.replicator.container.zookeeper.UuidOperator;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.InboundFilterChainContext;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.transaction.TransactionFilterChainFactory;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.EventTransactionCache;
import com.ctrip.framework.drc.replicator.store.AbstractTransactionTest;
import com.ctrip.framework.drc.replicator.store.FilePersistenceEventStore;
import com.ctrip.framework.drc.replicator.store.manager.gtid.DefaultGtidManager;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;
import static com.ctrip.framework.drc.core.driver.util.ByteHelper.FORMAT_LOG_EVENT_SIZE;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.EMPTY_DRC_UUID_EVENT_SIZE;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.EMPTY_PREVIOUS_GTID_EVENT_SIZE;
import static com.ctrip.framework.drc.replicator.store.manager.file.DefaultFileManager.LOG_EVENT_START;
import static com.ctrip.framework.drc.replicator.store.manager.file.DefaultFileManager.LOG_FILE_FORMAT;

/**
 * Created by mingdongli
 * 2019/9/18 下午2:05.
 */
public class DefaultFileManagerTest extends AbstractTransactionTest {

    public static final int FILTER_LOG_EVENT_SIZE = 45;
    private int purgeSize = 5;

    private GtidManager gtidManager;

    @Mock
    private SchemaManager schemaManager;

    @Mock
    private ReplicatorConfig replicatorConfig;

    @Mock
    private UuidOperator uuidOperator;

    @Mock
    private UuidConfig uuidConfig;

    private Filter<ITransactionEvent> filterChain = new TransactionFilterChainFactory().createFilterChain(
            new InboundFilterChainContext.Builder().applyMode(ApplyMode.transaction_table.getType()).build());

    private Set<UUID> uuids = Sets.newHashSet();

    @Before
    public void setUp() throws Exception {
        System.setProperty(SystemConfig.REPLICATOR_WHITE_LIST, String.valueOf(true));
        super.initMocks();
        when(replicatorConfig.getWhiteUUID()).thenReturn(uuids);
        when(replicatorConfig.getRegistryKey()).thenReturn("");
        when(uuidOperator.getUuids(anyString())).thenReturn(uuidConfig);
        when(uuidConfig.getUuids()).thenReturn(Sets.newHashSet());

        System.setProperty(SystemConfig.REPLICATOR_FILE_LIMIT, String.valueOf(1024 * 2));
        System.setProperty(SystemConfig.REPLICATOR_BINLOG_PURGE_SCALE_OUT, String.valueOf(purgeSize));
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

    @After
    public void tearDown() throws Exception {
        System.setProperty(SystemConfig.REPLICATOR_WHITE_LIST, String.valueOf(false));
        fileManager.stop();
        fileManager.dispose();
        fileManager.destroy();
    }

    @Test
    public void testAppendAndRead() throws Exception {
        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);
        writeTransactionThroughTransactionCache();

        GtidSet gtidSet = fileManager.getExecutedGtids();
        Assert.assertEquals(gtidSet.add(GTID), false);
    }

    /**
     * 15464 byte
     *
     * @throws InterruptedException
     */
    @Test
    public void testMultiThreadWrite() throws Exception {
        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);
        CountDownLatch latch = new CountDownLatch(3);
        AtomicInteger count = new AtomicInteger(0);
        doWrite(count, latch);
        doWrite(count, latch);
        doWrite(count, latch);
        latch.await(20, TimeUnit.SECONDS);
        Assert.assertEquals(100 * 3, count.get());
        List<File> files = FileUtil.sortDataDir(logDir.listFiles(), DefaultFileManager.LOG_FILE_PREFIX, false);
        int total = 0;
        for (File file : files) {
            total += file.length();
        }
        Assert.assertEquals(total, EMPTY_PREVIOUS_GTID_EVENT_SIZE * 13 + (LOG_EVENT_START + EMPTY_PREVIOUS_GTID_EVENT_SIZE + EMPTY_SCHEMA_EVENT_SIZE + EMPTY_DRC_UUID_EVENT_SIZE + FORMAT_LOG_EVENT_SIZE + DrcIndexLogEvent.FIX_SIZE) * files.size() + count.get() * ((GTID_ZISE) + TABLE_MAP_SIZE + WRITE_ROW_SIZE + XID_ZISE));
    }

    private void doWrite(AtomicInteger count, CountDownLatch latch) {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    for (int i = 0; i < 100; i++) {
                        writeTransaction();
                        count.addAndGet(1);
                    }
                } catch (Exception e) {

                } finally {
                    latch.countDown();
                }
            }
        });
    }

    @Test
    public void testPurge() {
        int FILE_SIZE = 10;
        File file = fileManager.getDataDir();
        for (int i = 1; i <= FILE_SIZE; ++i) {
            String fileName = String.format(LOG_FILE_FORMAT, "rbinlog", i);
            File f = new File(file.getAbsolutePath() + "/" + fileName);
            try {
                f.createNewFile();
            } catch (IOException e) {

            }
        }
        fileManager.purge();
        List<File> files = FileUtil.sortDataDir(file.listFiles(), DefaultFileManager.LOG_FILE_PREFIX, false);
        Assert.assertEquals(files.size(), purgeSize);

        for (int i = 0; i < files.size(); i++) {
            Assert.assertEquals(files.get(i).getName(), String.format(LOG_FILE_FORMAT, "rbinlog", FILE_SIZE - i));
            files.get(i).delete();
        }
    }

    @Test
    public void testTruncatePayload() throws Exception {
        List<File> files = FileUtil.sortDataDir(fileManager.getDataDir().listFiles(), DefaultFileManager.LOG_FILE_PREFIX, false);
        if (files == null || files.isEmpty()) {
            writeTransactionThroughTransactionCache();
        }

        fileManager.getExecutedGtids();  //会truncate
        int beforeSize = getTotalSize();

        File before = fileManager.getCurrentLogFile();
        String beforeName = "";
        if (before != null) {
            beforeName = before.getName();
        }

        writeTransactionThroughTransactionCache();
        writePartialTransactionThroughTransactionCache();

        GtidSet gtidSet = fileManager.getExecutedGtids();  //should truncate
        int afterSize = getTotalSize();

        File after = fileManager.getCurrentLogFile();
        String afterName = "";
        if (after != null) {
            afterName = after.getName();
        }

        if (!afterName.equalsIgnoreCase(beforeName)) {
            beforeSize = beforeSize + LOG_EVENT_START + EMPTY_PREVIOUS_GTID_EVENT_SIZE + FORMAT_LOG_EVENT_SIZE;
        }

        logger.info("after size is {} {}", afterSize, fileManager.getCurrentLogFile().getName());

        Assert.assertEquals(beforeSize, afterSize - ((GTID_ZISE + 4) + TABLE_MAP_SIZE + WRITE_ROW_SIZE + XID_ZISE + FILTER_LOG_EVENT_SIZE));
        Assert.assertEquals(gtidSet.add(GTID), false);
        Assert.assertEquals(gtidSet.add(TRUNCATED_GTID), true);
    }

    @Test
    public void testTruncateGtidHeader() throws Exception {
        List<File> files = FileUtil.sortDataDir(fileManager.getDataDir().listFiles(), DefaultFileManager.LOG_FILE_PREFIX, false);
        if (files == null || files.isEmpty()) {
            writeTransactionThroughTransactionCache();
        }

        fileManager.getExecutedGtids();  //not truncate
        int beforeSize = getTotalSize();

        File before = fileManager.getCurrentLogFile();
        String beforeName = "";
        if (before != null) {
            beforeName = before.getName();
        }

        writeTransactionThroughTransactionCache();
        writePartialGtidHeaderThroughFileManager();

        GtidSet gtidSet = fileManager.getExecutedGtids();  //should truncate
        int afterSize = getTotalSize();

        File after = fileManager.getCurrentLogFile();
        String afterName = "";
        if (after != null) {
            afterName = after.getName();
        }

        if (!afterName.equalsIgnoreCase(beforeName)) {
            beforeSize = beforeSize + LOG_EVENT_START + EMPTY_PREVIOUS_GTID_EVENT_SIZE + FORMAT_LOG_EVENT_SIZE;
        }

        logger.info("after size is {} {}", afterSize, fileManager.getCurrentLogFile().getName());

        Assert.assertEquals(beforeSize, afterSize - ((GTID_ZISE + 4) + TABLE_MAP_SIZE + WRITE_ROW_SIZE + XID_ZISE + FILTER_LOG_EVENT_SIZE));
        Assert.assertEquals(gtidSet.add(GTID), false);
        Assert.assertEquals(gtidSet.add(TRUNCATED_GTID), true);
    }

    @Test
    public void testTruncateNotGtidHeader() throws Exception {
        List<File> files = FileUtil.sortDataDir(fileManager.getDataDir().listFiles(), DefaultFileManager.LOG_FILE_PREFIX, false);
        if (files == null || files.isEmpty()) {
            writeTransactionThroughTransactionCache();
        }

        fileManager.getExecutedGtids();  //会truncate
        int beforeSize = getTotalSize();

        File before = fileManager.getCurrentLogFile();
        String beforeName = "";
        if (before != null) {
            beforeName = before.getName();
        }

        writeTransactionThroughTransactionCache();
        writePartialNotGtidHeaderThroughFileManager();

        GtidSet gtidSet = fileManager.getExecutedGtids();  //should truncate
        int afterSize = getTotalSize();

        File after = fileManager.getCurrentLogFile();
        String afterName = "";
        if (after != null) {
            afterName = after.getName();
        }

        if (!afterName.equalsIgnoreCase(beforeName)) {
            beforeSize = beforeSize + LOG_EVENT_START + EMPTY_PREVIOUS_GTID_EVENT_SIZE + FORMAT_LOG_EVENT_SIZE;
        }

        logger.info("after size is {} {}", afterSize, fileManager.getCurrentLogFile().getName());

        Assert.assertEquals(beforeSize, afterSize - ((GTID_ZISE + 4) + TABLE_MAP_SIZE + WRITE_ROW_SIZE + XID_ZISE + FILTER_LOG_EVENT_SIZE));
        Assert.assertEquals(gtidSet.add(GTID), false);
        Assert.assertEquals(gtidSet.add(TRUNCATED_GTID), true);
    }

    @Test
    public void testTruncatePartialFilterLogEvent() throws Exception {
        List<File> files = FileUtil.sortDataDir(fileManager.getDataDir().listFiles(), DefaultFileManager.LOG_FILE_PREFIX, false);
        if (files == null || files.isEmpty()) {
            writeTransactionThroughTransactionCache();
        }

        fileManager.getExecutedGtids();  //not truncate

        writeTransactionThroughTransactionCache();
        int correctSize = getTotalSize();

        int partialSize = writePartialFilterLogEventBodyThroughFileManager();
        int errorSize = getTotalSize();

        GtidSet gtidSet = fileManager.getExecutedGtids();  //should truncate
        int afterSize = getTotalSize();

        Assert.assertEquals(correctSize, afterSize);
        Assert.assertEquals(partialSize, errorSize - correctSize);
        Assert.assertFalse(gtidSet.add(GTID));
    }

    @Test
    public void testTruncateFilterLogEventBeforePartialGtid() throws Exception {
        List<File> files = FileUtil.sortDataDir(fileManager.getDataDir().listFiles(), DefaultFileManager.LOG_FILE_PREFIX, false);
        if (files == null || files.isEmpty()) {
            writeTransactionThroughTransactionCache();
        }

        fileManager.getExecutedGtids();  //not truncate

        writeTransactionThroughTransactionCache();
        int correctSize = getTotalSize();

        int filterLogEventSize = writeFilterLogEventThroughFileManager();
        int partialSize = writePartialGtidHeaderThroughFileManager();
        int errorSize = getTotalSize();

        GtidSet gtidSet = fileManager.getExecutedGtids();  //should truncate
        int afterSize = getTotalSize();

        Assert.assertEquals(correctSize, afterSize);
        Assert.assertEquals(filterLogEventSize + partialSize, errorSize - correctSize);
        Assert.assertFalse(gtidSet.add(GTID));
    }

    @Test
    public void testTruncateLastFilterLogEvent() throws Exception {
        List<File> files = FileUtil.sortDataDir(fileManager.getDataDir().listFiles(), DefaultFileManager.LOG_FILE_PREFIX, false);
        if (files == null || files.isEmpty()) {
            writeTransactionThroughTransactionCache();
        }

        fileManager.getExecutedGtids();  //not truncate

        writeTransactionThroughTransactionCache();
        int correctSize = getTotalSize();

        int partialSize = writeFilterLogEventThroughFileManager();
        int errorSize = getTotalSize();

        GtidSet gtidSet = fileManager.getExecutedGtids();  //should truncate
        int afterSize = getTotalSize();

        Assert.assertEquals(correctSize, afterSize);
        Assert.assertEquals(partialSize, errorSize - correctSize);
        Assert.assertFalse(gtidSet.add(GTID));
    }

    @Test
    public void testTruncateLastFilterLogEventHeader() throws Exception {
        List<File> files = FileUtil.sortDataDir(fileManager.getDataDir().listFiles(), DefaultFileManager.LOG_FILE_PREFIX, false);
        if (files == null || files.isEmpty()) {
            writeTransactionThroughTransactionCache();
        }

        fileManager.getExecutedGtids();  //not truncate

        writeTransactionThroughTransactionCache();
        int correctSize = getTotalSize();

        int headerSize = writeFilterLogEventHeaderThroughFileManager();
        int errorSize = getTotalSize();

        GtidSet gtidSet = fileManager.getExecutedGtids();  //should truncate
        int afterSize = getTotalSize();

        Assert.assertEquals(correctSize, afterSize);
        Assert.assertEquals(headerSize, errorSize - correctSize);
    }

    @Test
    public void testTruncateOnlyLastFilterLogEventHeader() throws Exception {
        List<File> files = FileUtil.sortDataDir(fileManager.getDataDir().listFiles(), DefaultFileManager.LOG_FILE_PREFIX, false);

        if (files == null || files.isEmpty()) {
            writeTransactionThroughTransactionCache();
        }

        fileManager.getExecutedGtids();  //not truncate

        writeTransactionThroughTransactionCache();
        int correctSize = getTotalSize();

        int headerSize = writeFilterLogEventHeaderThroughFileManager();
        int errorSize = getTotalSize();

        GtidSet gtidSet = fileManager.getExecutedGtids();  //should truncate
        int afterSize = getTotalSize();

        Assert.assertEquals(correctSize, afterSize);
        Assert.assertEquals(headerSize, errorSize - correctSize);
    }

    @Test
    public void testTruncatePartialLastFilterLogEventHeader() throws Exception {
        List<File> files = FileUtil.sortDataDir(fileManager.getDataDir().listFiles(), DefaultFileManager.LOG_FILE_PREFIX, false);

        if (files == null || files.isEmpty()) {
            writeTransactionThroughTransactionCache();
        }

        int beforeSize = getTotalSize();

        fileManager.getExecutedGtids();  //not truncate
        int correctSize = getTotalSize();
        Assert.assertEquals(beforeSize, correctSize);

        int headerSize = writePartialFilterLogEventHeaderThroughFileManager();
        int errorSize = getTotalSize();

        GtidSet gtidSet = fileManager.getExecutedGtids();  //should truncate
        int afterSize = getTotalSize();

        Assert.assertEquals(correctSize, afterSize);
        Assert.assertEquals(headerSize, errorSize - correctSize);
    }

    @Test
    public void testFilePosition() throws Exception {
        ByteBuf byteBuf = getGtidEventHearder();
        fileManager.append(byteBuf);
        File file = fileManager.getCurrentLogFile();
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        FileChannel fileChannel = raf.getChannel();
        long size = fileChannel.size();
        fileChannel.position(size - byteBuf.capacity());
        fileChannel.position(size + 1);
        Assert.assertTrue(fileChannel.size() < fileChannel.position());
    }

    @Test
    public void testGtidHeader() throws Exception {
        ByteBuf byteBuf = getGtidEventHearder();
        fileManager.append(byteBuf);
        byteBuf.release();

        File file = fileManager.getCurrentLogFile();
        long previousLength = file.length();
        fileManager.getExecutedGtids();  //会truncate
        long currentLength = file.length();
        Assert.assertEquals(previousLength - currentLength, eventHeaderLengthVersionGt1);

    }

    @Test
    public void testXidHeader() throws Exception {
        writeTransactionThroughTransactionCache();
        ByteBuf byteBuf = getXidEventHeader();
        fileManager.append(byteBuf);
        File file = fileManager.getCurrentLogFile();
        long previousLength = file.length();
        fileManager.getExecutedGtids();  //会truncate
        long currentLength = file.length();
        Assert.assertEquals(previousLength - currentLength, eventHeaderLengthVersionGt1);
    }

    @Test
    public void testRowsEventHeader() throws Exception {
        writeTransactionThroughTransactionCache();
        ByteBuf byteBuf = getGtidEvent();
        GtidLogEvent gtidLogEvent = new GtidLogEvent().read(byteBuf);
        gtidLogEvent.setNextTransactionOffsetAndUpdateEventSize(TABLE_MAP_SIZE + WRITE_ROW_SIZE + XID_ZISE);
        int gtidSize = byteBuf.capacity();
        fileManager.append(gtidLogEvent.getLogEventHeader().getHeaderBuf());
        fileManager.append(gtidLogEvent.getPayloadBuf());
        byteBuf.release();
        byteBuf = getMinimalRowsEventHeader();
        fileManager.append(byteBuf);
        byteBuf.release();
        File file = fileManager.getCurrentLogFile();
        long previousLength = file.length();
        fileManager.getExecutedGtids();  //will truncate
        long currentLength = file.length();
        Assert.assertEquals(previousLength - currentLength, eventHeaderLengthVersionGt1 + gtidSize + 4);
    }

    @Test
    public void getNextFile() {
        File current = new File(fileManager.getDataDir(), DefaultFileManager.LOG_FILE_PREFIX + ".000012");
        File next = fileManager.getNextLogFile(current);
        Assert.assertEquals(next.getName(), DefaultFileManager.LOG_FILE_PREFIX + ".0000000013");
    }

    @Test
    public void getFirstLogFile() throws Exception {
        List<File> files = FileUtil.sortDataDir(fileManager.getDataDir().listFiles(), DefaultFileManager.LOG_FILE_PREFIX, true);
        if (files == null || files.isEmpty()) {
            writeTransactionThroughTransactionCache();
            File file = fileManager.getFirstLogFile();
            Assert.assertEquals(file.getName(), DefaultFileManager.LOG_FILE_PREFIX + ".0000000001");
        } else {
            File file = files.get(0);
            File firstFile = fileManager.getFirstLogFile();
            Assert.assertEquals(file.getName(), firstFile.getName());
        }

    }

    @Test
    public void testDrcGtidLogEvent() {
        ByteBuf byteBuf = getGtidEvent();
        GtidLogEvent gtidLogEvent = new GtidLogEvent().read(byteBuf);
        gtidLogEvent.setEventType(LogEventType.drc_gtid_log_event.getType());
        String gtid = gtidLogEvent.getGtid();
        gtidLogEvent.write(byteBufs -> {
            try {
                fileManager.append(byteBufs, new TransactionContext(false));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        GtidSet gtidSet = new GtidSet("");
        Assert.assertTrue(gtidSet.add(gtid));
        gtidLogEvent.release();

    }

    @Test
    public void testGetReplicators() {
        List<String> replicators = DefaultFileManager.getReplicators(null);
        Assert.assertTrue(replicators.contains(DESTINATION));
    }

    @Test(expected = IllegalStateException.class)
    public void getCurrentLogFile() {
        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);
        fileManager.getCurrentLogFile();
    }

    @Test
    public void getCurrentLogFileNotException() throws Exception {
        writeTransactionThroughTransactionCache();
        File file = fileManager.getCurrentLogFile();
        Assert.assertNotNull(file);
    }

    @Test
    public void testFilter() throws Exception {
        String uuid1 = "c372080a-1804-11ea-8add-98039bbedf9c";
        String gtidSet = "560f4cad-8c39-11e9-b53b-6c92bf463216:1-134,c372080a-1804-11ea-8add-98039bbedf9c:1-1234";
        String testGtidSet = "c372080a-1804-11ea-8add-98039bbedf9c:1-1235";
        Set<String> uuids = Sets.newHashSet(uuid1);
        gtidManager.setUuids(uuids);
        GtidSet gtidSet1 = new GtidSet(gtidSet);
        gtidManager.updateExecutedGtids(gtidSet1);
        writeTransactionThroughTransactionCache();
        File file = fileManager.getFirstLogNotInGtidSet(new GtidSet(testGtidSet), false);
        Assert.assertNull(file);
        file = fileManager.getFirstLogNotInGtidSet(new GtidSet(testGtidSet), true);
        Assert.assertNotNull(file);

        testGtidSet = "560f4cad-8c39-11e9-b53b-6c92bf463216:1-134,c372080a-1804-11ea-8add-98039bbedf9c:1-1234";
        file = fileManager.getFirstLogNotInGtidSet(new GtidSet(testGtidSet), false);
        Assert.assertNotNull(file);
        file = fileManager.getFirstLogNotInGtidSet(new GtidSet(testGtidSet), true);
        Assert.assertNotNull(file);

        testGtidSet = "560f4cad-8c39-11e9-b53b-6c92bf463216:1-134";
        file = fileManager.getFirstLogNotInGtidSet(new GtidSet(testGtidSet), false);
        Assert.assertNull(file);
        file = fileManager.getFirstLogNotInGtidSet(new GtidSet(testGtidSet), true);
        Assert.assertNull(file);
    }

    @Test
    public void gtidExecuted() throws Exception {
        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);
        gtidManager.setUuids(Sets.newHashSet("c372080a-1804-11ea-8add-98039bbedf9c"));
        gtidManager.updateExecutedGtids(new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-1233,c372080a-1804-11ea-8add-98039bbedf12:1-12334567"));
        writeTransactionThroughTransactionCache();
        File currentFile = fileManager.getCurrentLogFile();
        boolean res = fileManager.gtidExecuted(currentFile, new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-1534"));
        Assert.assertFalse(res);

        fileManager.rollLog();
        gtidManager.updateExecutedGtids(new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-1534,c372080a-1804-11ea-8add-98039bbedf12:1-12334567"));
        writeTransactionThroughTransactionCache();
        res = fileManager.gtidExecuted(currentFile, new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-1634"));
        Assert.assertTrue(res);

        res = fileManager.gtidExecuted(currentFile, new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-1400:1402-1600"));
        Assert.assertFalse(res);

    }

    @Test
    public void bigTransaction() throws IOException {
        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);

        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer();
        List<ByteBuf> bigTransaction = getBigTransaction();
        long size = 0;
        for (ByteBuf byteBuf : bigTransaction) {
            byteBuf.readerIndex(0);
            size += byteBuf.readableBytes();
            compositeByteBuf.addComponent(true, byteBuf);
        }

        List<ByteBuf> events = new ArrayList<>();
        events.add(compositeByteBuf);
        fileManager.append(events, new TransactionContext(false, bigTransaction.size() / 2));
        Assert.assertTrue(((DefaultFileManager) fileManager).isInBigTransaction());

        List<File> files = FileUtil.sortDataDir(logDir.listFiles(), DefaultFileManager.LOG_FILE_PREFIX, false);
        int total = 0;
        for (File file : files) {
            total += file.length();
        }
        Assert.assertEquals(total, (LOG_EVENT_START + EMPTY_PREVIOUS_GTID_EVENT_SIZE + EMPTY_SCHEMA_EVENT_SIZE + EMPTY_DRC_UUID_EVENT_SIZE + FORMAT_LOG_EVENT_SIZE + DrcIndexLogEvent.FIX_SIZE) * files.size() + size);

    }

    private int getTotalSize() {
        List<File> files = FileUtil.sortDataDir(fileManager.getDataDir().listFiles(), DefaultFileManager.LOG_FILE_PREFIX, false);
        int total = 0;
        for (File file : files) {
            total += file.length();
        }
        return total;
    }

    private void writePartialTransaction() throws Exception {
        ByteBuf byteBuf = getPartialGtidEvent();
        fileManager.append(byteBuf);
        byteBuf = getCharsetTypeTableMapEvent();
        fileManager.append(byteBuf);
    }

    private void writePartialTransactionThroughTransactionCache() throws Exception {
        ByteBuf byteBuf = getGtidEvent();
        GtidLogEvent gtidLogEvent = new GtidLogEvent().read(byteBuf);
        transactionCache.add(gtidLogEvent);
        byteBuf.release();

        byteBuf = getCharsetTypeTableMapEvent();
        TableMapLogEvent tableMapLogEvent = new TableMapLogEvent().read(byteBuf);
        transactionCache.add(tableMapLogEvent);

        transactionCache.flush();
    }

    private int writePartialGtidHeaderThroughFileManager() throws Exception {
        ByteBuf byteBuf = getPartialGtidEventHearder();
        int size = byteBuf.writerIndex();
        fileManager.append(byteBuf);
        byteBuf.release();
        return size;
    }

    private void writePartialNotGtidHeaderThroughFileManager() throws Exception {
        ByteBuf byteBuf = getGtidEvent();
        fileManager.append(byteBuf);
        byteBuf.release();

        byteBuf = getCharsetTypeTableMapEvent();
        fileManager.append(byteBuf);
        byteBuf.release();

        byteBuf = getPartialMinimalRowsEventByteBuf();
        fileManager.append(byteBuf);
        byteBuf.release();
    }


    private int writeFilterLogEventThroughFileManager() throws Exception {
        ByteBuf byteBuf = getFilterLogEvent();
        int size = byteBuf.writerIndex();
        fileManager.append(byteBuf);
        byteBuf.release();
        return size;
    }

    protected ByteBuf getFilterLogEvent() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(16);
        byte[] bytes = new byte[]{
                (byte) 0x3d, (byte) 0x24, (byte) 0x5f, (byte) 0x65, (byte) 0x6d, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x24, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xa0, (byte) 0x01, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x0c, (byte) 0x64, (byte) 0x72, (byte) 0x63, (byte) 0x6d, (byte) 0x6f, (byte) 0x6e, (byte) 0x69, (byte) 0x74, (byte) 0x6f, (byte) 0x72, (byte) 0x64, (byte) 0x62, (byte) 0xa0, (byte) 0x01, (byte) 0x00,
                (byte) 0x00
        };
        byteBuf.writeBytes(bytes);
        return byteBuf;
    }

    private int writePartialFilterLogEventBodyThroughFileManager() throws Exception {
        ByteBuf byteBuf = getPartialFilterLogEventBody();
        int size = byteBuf.writerIndex();
        fileManager.append(byteBuf);
        byteBuf.release();
        return size;
    }

    protected ByteBuf getPartialFilterLogEventBody() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(16);
        byte[] bytes = new byte[]{
                (byte) 0x3d, (byte) 0x24, (byte) 0x5f, (byte) 0x65, (byte) 0x6d, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x24, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xa0, (byte) 0x01, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x0c, (byte) 0x64, (byte) 0x72, (byte) 0x63, (byte) 0x6d, (byte) 0x6f, (byte) 0x6e, (byte) 0x69, (byte) 0x74, (byte) 0x6f, (byte) 0x72, (byte) 0x64, (byte) 0x62, (byte) 0xa0, (byte) 0x01, (byte) 0x00
        };
        byteBuf.writeBytes(bytes);
        return byteBuf;
    }

    private int writeFilterLogEventHeaderThroughFileManager() throws Exception {
        ByteBuf byteBuf = getFilterLogEventHeader();
        int size = byteBuf.writerIndex();
        fileManager.append(byteBuf);
        byteBuf.release();
        return size;
    }

    protected ByteBuf getFilterLogEventHeader() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(16);
        byte[] bytes = new byte[]{
                (byte) 0x3d, (byte) 0x24, (byte) 0x5f, (byte) 0x65, (byte) 0x6d, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x24, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xa0, (byte) 0x01, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00
        };
        byteBuf.writeBytes(bytes);
        return byteBuf;
    }

    private int writePartialFilterLogEventHeaderThroughFileManager() throws Exception {
        ByteBuf byteBuf = getPartialFilterLogEventHeader();
        int size = byteBuf.writerIndex();
        fileManager.append(byteBuf);
        byteBuf.release();
        return size;
    }

    protected ByteBuf getPartialFilterLogEventHeader() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(16);
        byte[] bytes = new byte[]{
                (byte) 0x3d, (byte) 0x24, (byte) 0x5f, (byte) 0x65, (byte) 0x6d, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x24, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xa0, (byte) 0x01, (byte) 0x00, (byte) 0x00
        };
        byteBuf.writeBytes(bytes);
        return byteBuf;
    }
}
