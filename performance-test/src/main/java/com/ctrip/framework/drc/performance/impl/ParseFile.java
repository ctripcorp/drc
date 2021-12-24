package com.ctrip.framework.drc.performance.impl;

import com.ctrip.framework.drc.applier.activity.replicator.converter.ApplierByteBufConverter;
import com.ctrip.framework.drc.applier.activity.replicator.driver.ApplierPooledConnector;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.converter.AbstractByteBufConverter;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcDdlLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcIndexLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcSchemaSnapshotLogEvent;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.driver.config.MySQLSlaveConfig;
import com.ctrip.framework.drc.core.server.utils.FileUtil;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.fetcher.activity.replicator.FetcherSlaveServer;
import com.ctrip.framework.drc.performance.utils.ConfigService;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.*;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.unknown_log_event;

/**
 * Created by jixinwang on 2021/8/17
 */
public class ParseFile extends FetcherSlaveServer {

    private static final String PARSE_FILE_PATH = System.getProperty("parse.file.path", "/data/drc/replicator/test.consume");

    private static String RESET_MASTER = "reset master";

    public static final String LOG_FILE_PREFIX = "rbinlog";

    private static final int TRUNCATE_FLAG = -1;

    private static final int LOG_EVENT_START = 4;

    protected Endpoint endPoint;

    private int ROUND;

    private BlockingQueue<LogEvent> logEventQueue = new LinkedBlockingQueue<LogEvent>(100000);

    private ExecutorService executorService = Executors.newFixedThreadPool(1);

    private ScheduledExecutorService benchmarkScheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("benchmark-scheduledExecutorService");

    private LogEventHandler eventHandler;

    private File logDir;

    private ApplierTestSchemaManager schemaManager;

    protected AbstractByteBufConverter byteBufConverter;

//    private long putEventIndex = 0;
//
//    private long takeEventIndex = 0;

    private boolean firstLoadFile = true;

    public ParseFile(Endpoint endPoint) {
        super(new MySQLSlaveConfig(), new ApplierPooledConnector(new DefaultEndPoint("1.1.1.1", 1)), new ApplierByteBufConverter());
        this.endPoint = endPoint;
    }

    @Override
    protected void doInitialize() {
        ROUND = ConfigService.getInstance().getBinlogReplayRound();
        this.logDir = getDataDir();
        logger.info("[Performance] replay round is {}", ROUND);
        schemaManager = new ApplierTestSchemaManager(endPoint, 8383, "appliertest_dalcluster", null);
        schemaManager.doInitialize();
        byteBufConverter = new ApplierByteBufConverter();
    }

    @Override
    protected void doStart() {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    handleFiles();
                } catch (Exception e) {
                    logger.error("get event error", e);
                }
            }
        });

        benchmarkScheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                round();
            }
        }, 10, 100, TimeUnit.MILLISECONDS);
    }

    public void round() {
        int i = 0;
        while (i < ROUND) {
            try {
                LogEvent logEvent = logEventQueue.take();
                if (logEvent.getLogEventType() == LogEventType.gtid_log_event) {
                    i++;
                }
//                logger.info("takeEventIndex is: {}, current log event queue size is: {}", takeEventIndex++, logEventQueue.size());
                eventHandler.onLogEvent(logEvent, null, null);
            } catch (Throwable t) {
                logger.info("handle event error", t);
            }
        }
    }

    @Override
    protected void doStop() {
        executorService.shutdown();
        benchmarkScheduledExecutorService.shutdown();
    }

    public void setEventHandler(LogEventHandler eventHandler) {
        this.eventHandler = eventHandler;
    }

    public void getEvent(File file) {
        long truncatePosition = TRUNCATE_FLAG;
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
                    throw new IllegalStateException("Header read size is " + headerSize);
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

                if (LogEventType.drc_schema_snapshot_log_event == eventType || LogEventType.drc_ddl_log_event == eventType || LogEventType.drc_index_log_event == eventType) {
                    int bodySize = (int)eventSize - eventHeaderLengthVersionGt1;
                    ByteBuffer tmpBodyBuffer = ByteBuffer.allocateDirect(bodySize);
                    ByteBuf tmpBodyByteBuf = Unpooled.wrappedBuffer(tmpBodyBuffer);
                    fileChannel.read(tmpBodyBuffer);
                    compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer();
                    compositeByteBuf.addComponents(true, headerByteBuf, tmpBodyByteBuf);
                    if (LogEventType.drc_ddl_log_event == eventType) {
                        DrcDdlLogEvent ddlLogEvent = new DrcDdlLogEvent();
                        ddlLogEvent.read(compositeByteBuf);
                        schemaManager.apply(ddlLogEvent.getSchema(), ddlLogEvent.getDdl());
                        ddlLogEvent.release();
                    } else if (LogEventType.drc_schema_snapshot_log_event == eventType) {
                        if (firstLoadFile) {
                            DrcSchemaSnapshotLogEvent snapshotLogEvent = new DrcSchemaSnapshotLogEvent();
                            snapshotLogEvent.read(compositeByteBuf);
                            schemaManager.recovery(snapshotLogEvent);
                            initMetaSchema();
                            snapshotLogEvent.release();
                            firstLoadFile = false;
                        } else {
                            DrcSchemaSnapshotLogEvent snapshotLogEvent = new DrcSchemaSnapshotLogEvent();
                            snapshotLogEvent.read(compositeByteBuf);
                            snapshotLogEvent.release();
                            continue;
                        }
                    } else {
                        DrcIndexLogEvent indexLogEvent = new DrcIndexLogEvent();
                        indexLogEvent.read(compositeByteBuf);
                        indexLogEvent.release();
                    }
                    continue;
                }

                LogEvent logEvent = byteBufConverter.getNextEmptyLogEvent(headerByteBuf);
                if (logEvent == null) {
                    fileChannel.position(fileChannel.position() + eventSize - eventHeaderLengthVersionGt1);
                    continue;
                }

                ////read full event
                int bodySize = (int) eventSize - eventHeaderLengthVersionGt1;
                if (bodySize != previousBodySize) {
                    bodyBuffer = ByteBuffer.allocateDirect(bodySize);
                    previousBodySize = bodySize;
                }

                Pair<ByteBuf, Integer> bodyContent = readFile(fileChannel, bodyBuffer);
                ByteBuf bodyByteBuf = bodyContent.getKey();

                compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer();
                compositeByteBuf.addComponents(true, headerByteBuf, bodyByteBuf);
                logEvent.read(compositeByteBuf);
                logEventQueue.put(logEvent);
//                logger.info("putEventIndex is: {}", putEventIndex++);
            }
        } catch (Exception e) {
            logger.error("parse event error", e);
        }
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    protected void initMetaSchema() throws SQLException {
        DataSource dataSource = DataSourceManager.getInstance().getDataSource(endPoint);
        try (Connection connection = dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(RESET_MASTER);
            }
        }
    }

    private Pair<ByteBuf, Integer> readFile(FileChannel fileChannel, ByteBuffer byteBuffer) throws IOException {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(byteBuffer);
        int readSize = fileChannel.read(byteBuffer);
        byteBuffer.clear();
        return Pair.from(byteBuf, readSize);
    }

    public void handleFiles() {
        List<File> files = FileUtil.sortDataDir(logDir.listFiles(), LOG_FILE_PREFIX, true);
        if (!files.isEmpty()) {
            for (File file : files) {
                try {
                    getEvent(file);
                    logger.info("end file: " + file.getName());
                } catch(Exception e) {
                    logger.error("parse file exception is: {}", e.getCause().toString());
                }

            }
        }
    }

    public File getDataDir() {
        File logDir = new File(PARSE_FILE_PATH);
        if (!logDir.exists()) {
            boolean created = logDir.mkdirs();
            logger.info("create {} {}", logDir, created);
        }
        return logDir;
    }
}
