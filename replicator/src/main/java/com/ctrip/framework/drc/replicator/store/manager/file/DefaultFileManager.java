package com.ctrip.framework.drc.replicator.store.manager.file;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidConsumer;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidManager;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.impl.*;
import com.ctrip.framework.drc.core.driver.binlog.manager.SchemaManager;
import com.ctrip.framework.drc.core.driver.binlog.manager.TableInfo;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.observer.gtid.GtidObserver;
import com.ctrip.framework.drc.core.server.utils.FileUtil;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.EventTransactionCache;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.unknown_log_event;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.*;

/**
 * Created by mingdongli
 * 2019/9/18 9:47.
 */
public class DefaultFileManager extends AbstractLifecycle implements FileManager {

    private static final Logger logger = LoggerFactory.getLogger(DefaultFileManager.class);

    private static final int PURGE_FILE_PERIOD_MIN = 15;

    private static long PREVIOUS_GTID_BULK = 50 * 1024 * 1024;

    public static final String LOG_PATH = System.getProperty(SystemConfig.KEY_REPLICATOR_PATH, SystemConfig.REPLICATOR_PATH);

    public static final int FORMAT_LOG_EVENT_SIZE = 119;

    public static final String LOG_FILE_PREFIX = "rbinlog";

    public static final String LOG_FILE_FORMAT = "%s.%010d";

    public static final byte[] DRC_LOG_MAGIC = new byte[]{(byte)0xfe, (byte)0x62, (byte)0x69, (byte)0x6e};

    public static final int LOG_EVENT_START = 4;

    public static final int TRUNCATE_FLAG = -1;

    /**
     * The actual rbinlog size limit in bytes.
     */
    public static long BINLOG_SIZE_LIMIT = 1024 * 1024 * 512;

    public static long BINLOG_PURGE_SCALE_OUT = 80;

    private File logDir;

    private volatile FileChannel logChannel;

    private volatile RandomAccessFile raf;

    private volatile File logFileWrite;

    private AtomicLong logFileSize = new AtomicLong(0);

    private GtidManager gtidManager;

    private String registryKey;

    private SchemaManager schemaManager;

    // for skip gtids
    private List<Long> indices = Lists.newArrayList();

    private int indicesSize = 0;

    private long indexEventPosition = 0;

    private long firstPreviousGtidEventPosition = 0;

    private boolean everSeeDdl = false;

    private volatile boolean inBigTransaction = false;

    private List<Observer> observers = Lists.newCopyOnWriteArrayList();

    private ScheduledExecutorService flushService;

    private ScheduledExecutorService purgeService;

    public DefaultFileManager(SchemaManager schemaManager, String registryKey) {
        this.schemaManager = schemaManager;
        this.registryKey = registryKey;
    }

    @Override
    protected void doInitialize() {
        flushService = ThreadUtils.newSingleThreadScheduledExecutor("Flush-Binlog-File-" + registryKey);
        purgeService = ThreadUtils.newSingleThreadScheduledExecutor("Purge-Binlog-File-" + registryKey);

        this.logDir = getDataDir();
        String fileLimit = System.getProperty(SystemConfig.REPLICATOR_FILE_LIMIT);
        if (fileLimit != null) {
            BINLOG_SIZE_LIMIT = Long.parseLong(fileLimit);
        }

        String binlogPurgeScaleout = System.getProperty(SystemConfig.REPLICATOR_BINLOG_PURGE_SCALE_OUT);
        if (binlogPurgeScaleout != null) {
            BINLOG_PURGE_SCALE_OUT = Long.parseLong(binlogPurgeScaleout);
        }

        String previousGtidInterval = System.getProperty(SystemConfig.PREVIOUS_GTID_INTERVAL);
        if (previousGtidInterval != null) {
            PREVIOUS_GTID_BULK = Long.parseLong(previousGtidInterval);
        }
    }

    @Override
    protected void doStart() {
        startFlush(registryKey);
    }

    public synchronized boolean append(ByteBuf byteBuf) throws IOException {
        List<ByteBuf> byteBufs = new ArrayList<>();
        byteBufs.add(byteBuf);
        return this.append(byteBufs, new TransactionContext(false));
    }

    @Override
    public synchronized boolean append(Collection<ByteBuf> byteBufs, TransactionContext context) throws IOException {

        createFileIfNecessary();

        if (context.isDdl() && !everSeeDdl) {
            everSeeDdl = true;
        }
        checkIndices(false, context.getEventSize() == EventTransactionCache.bufferSize);

        int totalSize = 0;

        for (ByteBuf byteBuf : byteBufs) {
            if (byteBuf instanceof CompositeByteBuf) {
                byteBuf.readerIndex(0);
                byteBuf = Unpooled.wrappedBuffer(byteBuf);
            }
            int endIndex = byteBuf.writerIndex();
            ByteBuffer byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
            logChannel.write(byteBuffer);
            totalSize += endIndex;
        }

        logFileSize.addAndGet(totalSize);
        if (totalSize > eventHeaderLengthVersionGt1) {
            for (Observer observer : observers) {
                if (observer instanceof GtidObserver) {
                    observer.update(logChannel.position(), this);
                }
            }
        }

        return true;
    }

    @Override
    public File getDataDir() {
        File logDir = new File(LOG_PATH + registryKey);
        if (!logDir.exists()) {
            boolean created = logDir.mkdirs();
            logger.info("create {} {}", logDir, created);
        }
        return logDir;
    }

    @Override
    public File getCurrentLogFile() {
        if (logFileWrite != null) {
            return logFileWrite;
        }
        List<File> files = FileUtil.sortDataDir(logDir.listFiles(), DefaultFileManager.LOG_FILE_PREFIX, false);
        if (files != null && !files.isEmpty()) {
            return files.get(0);
        }
        throw new IllegalStateException("[Blank] binlog file");
    }

    @Override
    public File getFirstLogFile() {
        List<File> files = FileUtil.sortDataDir(logDir.listFiles(), DefaultFileManager.LOG_FILE_PREFIX, true);
        if (files != null && !files.isEmpty()) {
            return files.get(0);
        }
        return null;
    }

    @Override
    public File getNextLogFile(File current) {
        long fileNum = FileUtil.getFileNumFromName(current.getName(), LOG_FILE_PREFIX);
        fileNum++;
        String fileName = String.format(LOG_FILE_FORMAT, LOG_FILE_PREFIX, fileNum);
        File nextFile = new File(logDir, fileName);
        return nextFile;
    }

    @Override
    public boolean gtidExecuted(File currentFile, GtidSet executedGtid) {
        try {
            File nextFile = getNextLogFile(currentFile);
            if (!nextFile.exists()) {
                return false;
            }
            GtidSet currentExecutedGtidSet = doGetGtids(currentFile, false);
            GtidSet nextExecutedGtidSet = doGetGtids(nextFile, false);
            GtidSet executedGtidInCurrentFile = nextExecutedGtidSet.subtract(currentExecutedGtidSet);
            logger.info("executedGtidInCurrentFile {}, executedGtid {} for current file {}", executedGtidInCurrentFile, executedGtid, currentFile.getName());
            executedGtidInCurrentFile = executedGtidInCurrentFile.intersectionGtidSet(executedGtid);
            logger.info("filtered executedGtidInCurrentFile {}, executedGtid {} for current file {}", executedGtidInCurrentFile, executedGtid, currentFile.getName());
            return executedGtidInCurrentFile.isContainedWithin(executedGtid);
        } catch (Throwable t) {
            logger.error("gtidExecuted error", t);
            return false;
        }
    }

    @Override
    public void setGtidManager(GtidManager gtidManager) {
        this.gtidManager = gtidManager;
    }

    @Override
    public GtidSet getExecutedGtids() {
        List<File> files = FileUtil.sortDataDir(logDir.listFiles(), DefaultFileManager.LOG_FILE_PREFIX, false);
        GtidSet gtidSet = new GtidSet(Maps.newLinkedHashMap());
        if (!files.isEmpty()) {
            logger.info("[Start] restore executed gtidset for {}", registryKey);
            gtidSet = doGetGtids(files.get(0), true);
            logger.info("[Stop] restore executed gtidset for {}", registryKey);
        }
        return gtidSet;
    }

    @Override
    public GtidSet getPurgedGtids() {
        List<File> files = FileUtil.sortDataDir(logDir.listFiles(), DefaultFileManager.LOG_FILE_PREFIX, false);
        GtidSet gtidSet = new GtidSet(Maps.newLinkedHashMap());
        if (!files.isEmpty()) {
            gtidSet = doGetGtids(files.get(files.size() - 1), false);
        }
        return gtidSet;
    }

    @Override
    public File getFirstLogNotInGtidSet(GtidSet gtidSet, boolean onlyLocalUuids) {
        List<File> files = FileUtil.sortDataDir(logDir.listFiles(), DefaultFileManager.LOG_FILE_PREFIX, false);
        File firstFile = null;
        if (files != null && !files.isEmpty()) {
            for (File file : files) {
                try {
                    GtidSet previousGtidSet = doGetGtids(file, false);
                    if (previousGtidSet != null) {
                        logger.info("[Previous-GtidSet] before filter is {}", previousGtidSet);
                        if (onlyLocalUuids) {
                            previousGtidSet = previousGtidSet.filterGtid(gtidManager.getUuids());
                            logger.info("[Previous-GtidSet] after filter is {}", previousGtidSet);
                        }
                        if (previousGtidSet.isContainedWithin(gtidSet)) {
                            return file;
                        }
                    } else {
                        if ("true".equalsIgnoreCase(System.getProperty(REPLICATOR_FILE_FIRST))) {
                            firstFile = file;
                            logger.info("[First] file set to {}", file.getName());
                        }
                    }
                } catch (Exception e) {
                    logger.error("getFirstLogNotInGtidSet error", e);
                }
            }
        }
        return firstFile;
    }

    private Pair<ByteBuf, Integer> readFile(FileChannel fileChannel, ByteBuffer byteBuffer) throws IOException {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(byteBuffer);
        int readSize = fileChannel.read(byteBuffer);
        byteBuffer.clear();
        return Pair.from(byteBuf, readSize);
    }

    private GtidSet doGetGtids(File file, boolean executed) {
        long truncatePosition = TRUNCATE_FLAG;
        GtidConsumer gtidEventConsumer = new GtidConsumer(executed, true);
        String gtid = StringUtils.EMPTY;
        RandomAccessFile raf = null;
        ByteBuffer headBuffer = ByteBuffer.allocateDirect(eventHeaderLengthVersionGt1);
        ByteBuffer bodyBuffer = null;
        int previousBodySize = -1;
        try {

            raf = new RandomAccessFile(file, "rw");
            FileChannel fileChannel = raf.getChannel();
            if (fileChannel.position() == 0) {
                fileChannel.position(LOG_EVENT_START);
            }
            final long endPos = fileChannel.size();
            while (endPos > fileChannel.position()) {
                Pair<ByteBuf, Integer> headerContent = readFile(fileChannel, headBuffer);
                ByteBuf headerByteBuf = headerContent.getKey();
                int headerSize = headerContent.getValue();
                if (eventHeaderLengthVersionGt1 != headerSize) {
                    logger.error("Header read size is {} for {}", headerSize, registryKey);
                    if (truncatePosition == TRUNCATE_FLAG) {
                        truncatePosition = fileChannel.position() - headerSize;
                        logger.info("[TruncatePosition] set to {} for {} due to corrupted gtid header", truncatePosition, file.getName(), truncatePosition);
                    } else {
                        logger.info("[TruncatePosition] set to {} for {} due to corrupted header", truncatePosition, file.getName(), truncatePosition);
                    }
                    break;
                }

                CompositeByteBuf compositeByteBuf;
                LogEventType eventType = LogEventUtils.parseNextLogEventType(headerByteBuf);
                if (unknown_log_event == eventType) {
                    logger.error("read unknown_log_event and begin to truncate");
                    if (truncatePosition == TRUNCATE_FLAG) {
                        truncatePosition = fileChannel.position() - eventHeaderLengthVersionGt1;
                        logger.error("[Truncate] position set to {}", truncatePosition);
                    } else {
                        logger.error("[Truncate] position remind to {}", truncatePosition);
                    }
                    break;
                }
                long eventSize = LogEventUtils.parseNextLogEventSize(headerByteBuf);
                if (LogEventUtils.isGtidLogEvent(eventType) || LogEventType.previous_gtids_log_event == eventType) {
                    //1、read full event
                    int bodySize = (int)eventSize - eventHeaderLengthVersionGt1;
                    if (bodySize != previousBodySize) {
                        bodyBuffer = ByteBuffer.allocateDirect(bodySize);
                        previousBodySize = bodySize;
                    }

                    Pair<ByteBuf, Integer> bodyContent = readFile(fileChannel, bodyBuffer);
                    ByteBuf bodyByteBuf = bodyContent.getKey();
                    int readSize = bodyContent.getValue();
                    if (readSize != eventSize - eventHeaderLengthVersionGt1) {
                        if (readSize < 0) {
                            readSize = 0;
                        }
                        truncatePosition = endPos - readSize - eventHeaderLengthVersionGt1;
                        break;  //read to tail
                    }
                    compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer();
                    compositeByteBuf.addComponents(true, headerByteBuf, bodyByteBuf);
                    //2、add gtid
                    try {
                        if (LogEventUtils.isGtidLogEvent(eventType)) {
                            GtidLogEvent gtidLogEvent = new GtidLogEvent();
                            gtidLogEvent.read(compositeByteBuf);
                            long nextTransactionOffset = gtidLogEvent.getNextTransactionOffset();
                            if (fileChannel.position() + nextTransactionOffset <= endPos) {  //one transaction or just drc_gtid_log_event
                                if (nextTransactionOffset > 0 || LogEventUtils.isDrcGtidLogEvent(eventType)) {
                                    fileChannel.position(fileChannel.position() + nextTransactionOffset);
                                    gtidEventConsumer.offer(gtidLogEvent);
                                    if (logger.isDebugEnabled()) {
                                        logger.debug("[Position] skip {} for gtid {}, cluster {}", nextTransactionOffset, gtidLogEvent.getGtid(), registryKey);
                                    }
                                    truncatePosition = TRUNCATE_FLAG; //no need truncate
                                } else {
                                    gtid = gtidLogEvent.getGtid();
                                    truncatePosition = fileChannel.position() - eventSize;
                                }
                            } else {
                                gtid = gtidLogEvent.getGtid();
                                truncatePosition = fileChannel.position() - eventSize;
                            }
                        } else {
                            PreviousGtidsLogEvent previousGtidsLogEvent = new PreviousGtidsLogEvent();
                            previousGtidsLogEvent.read(compositeByteBuf);
                            GtidSet gtidSet = previousGtidsLogEvent.getGtidSet();
                            if (!executed) {
                                return gtidSet;
                            } else {
                                gtidEventConsumer.init(gtidSet);
                            }
                        }
                    } finally {
                        compositeByteBuf.release(compositeByteBuf.refCnt());
                    }
                } else if (executed && (LogEventType.xid_log_event == eventType)){
                    if (gtid != StringUtils.EMPTY) {  //add gtid when read xid
                        gtidEventConsumer.add(gtid);
                        gtid = StringUtils.EMPTY;
                    }
                    truncatePosition = TRUNCATE_FLAG;
                    fileChannel.position(fileChannel.position() + eventSize - eventHeaderLengthVersionGt1);
                } else if (executed && (LogEventType.drc_schema_snapshot_log_event == eventType || LogEventType.drc_ddl_log_event == eventType || LogEventType.drc_index_log_event == eventType)){
                    int bodySize = (int)eventSize - eventHeaderLengthVersionGt1;
                    ByteBuffer tmpBodyBuffer = ByteBuffer.allocateDirect(bodySize);
                    ByteBuf tmpBodyByteBuf = Unpooled.wrappedBuffer(tmpBodyBuffer);
                    int readSize = fileChannel.read(tmpBodyBuffer);
                    compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer();
                    compositeByteBuf.addComponents(true, headerByteBuf, tmpBodyByteBuf);
                    if (LogEventType.drc_ddl_log_event == eventType) {
                        DrcDdlLogEvent ddlLogEvent = new DrcDdlLogEvent();
                        ddlLogEvent.read(compositeByteBuf);
                        schemaManager.apply(ddlLogEvent.getSchema(), ddlLogEvent.getDdl());
                        ddlLogEvent.release();
                    } else if (LogEventType.drc_schema_snapshot_log_event == eventType) {
                        DrcSchemaSnapshotLogEvent snapshotLogEvent = new DrcSchemaSnapshotLogEvent();
                        snapshotLogEvent.read(compositeByteBuf);
                        schemaManager.recovery(snapshotLogEvent);
                        snapshotLogEvent.release();
                    } else {
                        DrcIndexLogEvent indexLogEvent = new DrcIndexLogEvent();
                        indexLogEvent.read(compositeByteBuf);
                        List<Long> localIndices = indexLogEvent.getIndices();
                        int previousGtidSize = localIndices.size();
                        if (previousGtidSize > 1) {
                            long position = localIndices.get(previousGtidSize - 1);
                            if (position > localIndices.get(0)) {  // equal when has ddl
                                fileChannel.position(position);
                                logger.info("[Position] skip to {} for {}", position, registryKey);
                            }
                        }
                        indexLogEvent.release();
                    }
                } else {
                    fileChannel.position(fileChannel.position() + eventSize - eventHeaderLengthVersionGt1);
                }
            }

            if (executed && truncatePosition != TRUNCATE_FLAG) {
                fileChannel.truncate(truncatePosition);
                logger.info("[Truncate] file {} at position {} due to no xid event", file.getName(), truncatePosition);
            } else if (endPos != fileChannel.position()) {
                if (endPos > eventHeaderLengthVersionGt1) {
                    fileChannel.truncate(endPos - eventHeaderLengthVersionGt1);
                    logger.info("[Truncate] file {} at position {} due to only header", file.getName(), endPos - eventHeaderLengthVersionGt1);
                } else {
                    logger.info("[Truncate] {} failed: endPos {} < {}", file.getName(), endPos ,eventHeaderLengthVersionGt1);
                    throw new IllegalStateException("file enPos error");
                }
            }
        } catch (IOException e) {
            logger.error("doGetGtids error", e);
        } finally {
            if (raf != null) {
                try {
                    raf.close();
                } catch (IOException e) {
                    logger.error("raf close error", e);
                }
            }
        }

        return gtidEventConsumer.getGtidEventSet();
    }

    private void createFileIfNecessary() throws IOException {
        if (logChannel == null && getLifecycleState().isStarted()) {
            long fileNum = 0;
            //files desc
            List<File> files = FileUtil.sortDataDir(logDir.listFiles(), LOG_FILE_PREFIX, false);
            if (!files.isEmpty()) {
                File latestFile = files.get(0);
                fileNum = FileUtil.getFileNumFromName(latestFile.getName(), LOG_FILE_PREFIX);
            }
            fileNum++;  //create new binlog when restart
            if ("true".equalsIgnoreCase(System.getProperty(REVERSE_REPLICATOR_SWITCH_TEST))) {
                fileNum--;
                doCreateFileForTest(fileNum);
            } else {
                doCreateFile(fileNum);
            }
        }
    }

    /**
     * previous_log_event [filter by replicator slave]
     * drc_table_map_log_event [filter by replicator slave]
     * drc_schema_snapshot_log_event [IMPORTANT:first event to init embedded db schema for replicator slave]
     * drc_uuid_log_event
     * @param fileNum
     * @throws IOException
     */

    private void doCreateFile(long fileNum) throws IOException {
        String fileName = String.format(LOG_FILE_FORMAT, LOG_FILE_PREFIX, fileNum);
        logFileWrite = new File(logDir, fileName);
        raf = new RandomAccessFile(logFileWrite, "rw");
        logChannel = raf.getChannel();
        FileHeader fileHeader = new FileHeader(DRC_LOG_MAGIC);
        byte[] fileHeaderBytes = fileHeader.toBytes();
        logFileSize.set(0);
        logFileSize.addAndGet(fileHeaderBytes.length);
        logChannel.write(ByteBuffer.wrap(fileHeaderBytes));
        writeFormatDescriptionLogEvent();
        firstPreviousGtidEventPosition = logChannel.position();
        writePreviousGtid();
        writeSchema();
        writeUuids();
        checkIndices(true, false);
        logChannel.force(true);
    }

    private void doCreateFileForTest(long fileNum) throws IOException {
        String fileName = String.format(LOG_FILE_FORMAT, LOG_FILE_PREFIX, fileNum);
        logFileWrite = new File(logDir, fileName);
        raf = new RandomAccessFile(logFileWrite, "rw");
        logFileSize.set(0);
        logChannel = raf.getChannel();
    }

    private void writeFormatDescriptionLogEvent() {
        try {
            logger.info("[Write] format description log event");
            ByteBuf byteBuf = getFormatDescriptionLogEvent();
            logFileSize.addAndGet(byteBuf.writerIndex());
            logChannel.write(byteBuf.nioBuffer(0, byteBuf.writerIndex()));
            byteBuf.release();
        } catch (Exception e) {
            logger.error("write FormatDescriptionLogEvent error", e);
        }
    }

    private void writePreviousGtid() {
        try {
            GtidSet gtidSet = gtidManager.getExecutedGtids();
            logger.info("[Generate] executed gtid {}", gtidSet.toString());
            PreviousGtidsLogEvent gtidsLogEvent = new PreviousGtidsLogEvent(0, logChannel.position(), gtidSet);
            doWriteLogEvent(gtidsLogEvent);
        } catch (Exception e) {
            logger.error("writePreviousGtid error", e);
        }
    }

    private void writeUuids() {
        Set<String> uuids = gtidManager.getUuids();
        try {
            logger.info("[Generate] drc_uuid_log_event for {}:{}", registryKey, uuids);
            DrcUuidLogEvent uuidLogEvent = new DrcUuidLogEvent(uuids, 0, logChannel.position());
            doWriteLogEvent(uuidLogEvent);
        } catch (Exception e) {
            logger.error("writeUuids error for {}:{}", registryKey, uuids, e);
        }
    }

    private void writeSchema() throws IOException {
        Map<String, Map<String, String>> snapshot = schemaManager.snapshot();
        if (snapshot.isEmpty()) {
            logger.error("[Schema] is empty, fatal error for {}", registryKey);
            DefaultEventMonitorHolder.getInstance().logAlertEvent("Empty Schema");
            if (isIntegrityTest()) {
                return;
            }
            throw new IllegalStateException("Empty schema");
        }
        for (Map.Entry<String, Map<String, String>> entry : snapshot.entrySet()) {
            String dbName = entry.getKey();
            Map<String, String> tables = entry.getValue();
            for (Map.Entry<String, String> table : tables.entrySet()) {
                TableInfo tableInfo = schemaManager.find(dbName, table.getKey());
                if (tableInfo != null) {
                    schemaManager.persistColumnInfo(tableInfo, true);
                }
            }
        }

        DrcSchemaSnapshotLogEvent schemaSnapshotLogEvent = new DrcSchemaSnapshotLogEvent(snapshot, 0 , logChannel.position());
        doWriteLogEvent(schemaSnapshotLogEvent);
        logger.info("[Persist] drc schema log event for {}", registryKey);
    }

    private void checkIndices(boolean append, boolean bigTransaction) {  //write previous event and index event
        try {
            long position = logFileSize.get();
            if (append) {  //new file and append DrcIndexLogEvent
                indices.clear();
                indicesSize = 0;
                everSeeDdl = false;
                indexEventPosition = logChannel.position();
                indices.add(firstPreviousGtidEventPosition);
                DrcIndexLogEvent indexLogEvent = new DrcIndexLogEvent(indices, 0 , position);
                doWriteLogEvent(indexLogEvent);
                logger.info("[Persist] drc index log event {} for {} at position {} of file {} and clear indicesSize", indices, registryKey, indexEventPosition, logFileWrite.getName());
            } else {
                if (!bigTransaction && position / PREVIOUS_GTID_BULK > indicesSize && !inBigTransaction) {
                    writePreviousGtid();
                    if (everSeeDdl) {
                        long previousPosition = position;
                        position = indices.get(indices.size() - 1);
                        logger.info("[Update] index position from {} to {} of file {}", previousPosition, position, logFileWrite.getName());
                    }
                    indices.add(position);
                    indicesSize++;
                    FileLock indexLock = logChannel.lock(indexEventPosition, DrcIndexLogEvent.FIX_SIZE, true);
                    long currentPosition = logChannel.position();
                    logChannel.position(indexEventPosition);
                    DrcIndexLogEvent indexLogEvent = new DrcIndexLogEvent(indices, 0 , position);
                    doWriteLogEvent(indexLogEvent, false);
                    indexLock.release();
                    logChannel.position(currentPosition);
                    logger.info("[Persist] drc index log event {} for {} at position {} of file {}", indices, registryKey, position, logFileWrite.getName());
                }
            }
        } catch (Exception e) {
            logger.error("writeIndex error", e);
        } finally {
            inBigTransaction = bigTransaction;
        }
    }

    private void doWriteLogEvent(LogEvent logEvent) {
        this.doWriteLogEvent(logEvent, true);
    }

    private void doWriteLogEvent(LogEvent logEvent, boolean append) {

        try {
            logEvent.write(byteBufs -> {
                for (ByteBuf byteBuf : byteBufs) {
                    try {
                        if (append) {
                            logFileSize.addAndGet(byteBuf.writerIndex());
                        }
                        logChannel.write(byteBuf.nioBuffer(0, byteBuf.writerIndex()));
                    } catch (IOException e) {
                        logger.error("write previous gtid set error", e);
                    }
                }
            });

            // release
            logEvent.release();
        } catch (Exception e) {
            logger.error("write LogEvent error", e);
        }
    }


    /**
     * flush periodically
     *
     * @throws IOException
     */
    @Override
    public synchronized void flush() throws IOException {
        // Roll the log file if we exceed the size limit
        long logSize = getCurrentLogSize();

        if (logSize > BINLOG_SIZE_LIMIT && !inBigTransaction) {
            rollLog();
            this.logFileSize.set(0);
            logger.info("rbinlog size limit reached : {} and clear logFileSize", logSize);
        }
    }

    /**
     * Return the current on-disk size of log size. This will be accurate only
     * after commit() is called. Otherwise, unflushed txns may not be included.
     */
    private long getCurrentLogSize() {
        if (logFileWrite != null) {
            return logFileSize.get();
        }
        return 0;
    }

    @VisibleForTesting
    public void rollLog() throws IOException {
        if (logChannel != null) {
            this.logChannel.force(true);
            this.raf.close();
            this.logChannel = null;
        }
    }

    private void startFlush(String destination) {
        logger.info("start flush registryKey {} periodically", destination);
        Random random = new Random();

        int flushPeriod = 1000;
        int flushInitialDelay = random.nextInt(flushPeriod);
        logger.info("[Flush] {} with initialDelay {}ms", destination, flushInitialDelay);
        flushService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    flush();
                } catch (IOException e) {
                    logger.error("flush error for {}", destination, e);
                }
            }
        }, flushInitialDelay, flushPeriod, TimeUnit.MILLISECONDS);

        logger.info("start purge registryKey {} periodically", destination);

        int purgePeriod = PURGE_FILE_PERIOD_MIN * 60 * 1000;
        int purgeInitialDelay = random.nextInt(purgePeriod);
        logger.info("[Purge] {} with initialDelay {}ms", destination, purgeInitialDelay);

        purgeService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                purge();
            }
        }, purgeInitialDelay, purgePeriod, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void doDispose() throws Exception {
        if (logChannel != null) {
            logChannel.close();
            logFileSize.set(0);
        }
    }

    @Override
    protected void doStop() throws Exception{
        if (logChannel != null) {
            logChannel.force(true);
        }
        flushService.shutdown();
        purgeService.shutdown();
    }

    /**
     * delete data physically, for test normally
     *
     */
    @Override
    public void destroy() {
        File[] files = logDir.listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            logger.info("delete file {}", file.getName());
            file.delete();
        }

        logger.info("delete directory {}", logDir.getName());
        logDir.delete();
    }

    @Override
    public void addObserver(Observer observer) {
        if (!observers.contains(observer)) {
            observers.add(observer);
        }
    }

    @Override
    public void removeObserver(Observer observer) {
        observers.remove(observer);
    }

    @Override
    public void purge() {
        try {
            List<File> files = FileUtil.sortDataDir(logDir.listFiles(), DefaultFileManager.LOG_FILE_PREFIX, false);
            if (files == null || files.size() < BINLOG_PURGE_SCALE_OUT) {
                logger.info("[Purge] return for not reaching limit {}", BINLOG_PURGE_SCALE_OUT);
                return;
            }
            List<File> toBePurged = Lists.newArrayList();
            for (int i = 0; i < files.size(); ++i) {
                if (i < BINLOG_PURGE_SCALE_OUT) {
                    continue;
                }
                toBePurged.add(files.get(i));
            }
            for (int i = toBePurged.size() - 1; i >= 0; i--) {
                logger.info("[Purge] file {} for {} to be start", toBePurged.get(i).getName(), registryKey);
                boolean deleted = toBePurged.get(i).delete();
                logger.info("[Purge] file {} for {} reaching maxFileSize {} with result {}", toBePurged.get(i).getName(), registryKey, BINLOG_PURGE_SCALE_OUT, deleted);
                Thread.sleep(20);
            }

            if (!toBePurged.isEmpty()) {
                updatePurgedGtid();
            }
        } catch (Exception e) {
            logger.error("[Purge] error for {}", registryKey, e);
        }
    }

    private boolean updatePurgedGtid() {
        GtidSet purgedGtid = getPurgedGtids();
        if (StringUtils.isBlank(purgedGtid.toString())) {
            logger.info("[Purged] error, blank previous gtid set");
            return false;
        }
        gtidManager.updatePurgedGtids(purgedGtid);
        logger.info("[Purged] gtid updated to {}", purgedGtid);
        return true;
    }

    public static List<String> getReplicators(String dir) {
        if (StringUtils.isBlank(dir)) {
            dir = LOG_PATH;
        }
        List<String> res = Lists.newArrayList();
        File logDir = new File(dir);
        File[] files = logDir.listFiles();
        if (files != null && files.length > 0) {
            for (File file : files) {
                res.add(file.getName());
            }
        }
        return res;
    }

    private ByteBuf getFormatDescriptionLogEvent() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(FORMAT_LOG_EVENT_SIZE);
        byte[] bytes = new byte[] {
                (byte) 0x6d, (byte) 0xe3, (byte) 0x7c, (byte) 0x5d,
                (byte) 0x0f, (byte) 0x01, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x77, (byte) 0x00, (byte) 0x00,

                (byte) 0x00, (byte) 0x7b, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x04,
                (byte) 0x00, (byte) 0x35, (byte) 0x2e, (byte) 0x37, (byte) 0x2e, (byte) 0x32, (byte) 0x37, (byte) 0x2d,

                (byte) 0x6c, (byte) 0x6f, (byte) 0x67, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x13,

                (byte) 0x38, (byte) 0x0d, (byte) 0x00, (byte) 0x08, (byte) 0x00, (byte) 0x12, (byte) 0x00, (byte) 0x04,
                (byte) 0x04, (byte) 0x04, (byte) 0x04, (byte) 0x12, (byte) 0x00, (byte) 0x00, (byte) 0x5f, (byte) 0x00,

                (byte) 0x04, (byte) 0x1a, (byte) 0x08, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x08, (byte) 0x08,
                (byte) 0x08, (byte) 0x02, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x0a, (byte) 0x0a, (byte) 0x0a,

                (byte) 0x2e, (byte) 0x2a, (byte) 0x00, (byte) 0x12, (byte) 0x34, (byte) 0x00, (byte) 0x01, (byte) 0xbf,
                (byte) 0xa0, (byte) 0xb5, (byte) 0xc4
        };
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    @VisibleForTesting
    public boolean isInBigTransaction() {
        return inBigTransaction;
    }
}
