package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.applier.activity.replicator.converter.ApplierByteBufConverter;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.converter.AbstractByteBufConverter;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.fetcher.activity.event.ApplierGroupActivity;
import com.ctrip.framework.drc.fetcher.event.ApplierGtidEvent;
import com.ctrip.framework.drc.fetcher.event.ApplierTableMapEvent;
import com.ctrip.framework.drc.fetcher.event.ApplierXidEvent;
import com.ctrip.framework.drc.fetcher.event.FetcherRowsEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.BaseBeginEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.Transaction;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionEvent;
import com.ctrip.framework.drc.fetcher.resource.condition.ListenableDirectMemory;
import com.ctrip.framework.drc.fetcher.resource.condition.ListenableDirectMemoryResource;
import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.unknown_log_event;

/**
 * 模拟从 binlog 文件读取事件并组装成 ApplyTransaction 结构体
 */
public class BinlogToTransactionTest {

    private static final String BINLOG_FILE_PATH = System.getProperty("test.binlog.path", "/Users/shiruixin/Documents/学习/applier-oom/binlog/rbinlog/rbinlog.0000024136");
    private static final int LOG_EVENT_START = 4;

    private AbstractByteBufConverter byteBufConverter;
    private MockApplierGroupActivity mockApplierGroupActivity;
    private int cnt = 0;
    private long allEventSize = 0;
    private ListenableDirectMemory directMemory;


    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        cnt = 0;
        
        // 初始化核心组件
        byteBufConverter = new ApplierByteBufConverter();
        mockApplierGroupActivity = new MockApplierGroupActivity();
        directMemory = new ListenableDirectMemoryResource();
    }

    @Test
    public void testBinlogToApplyTransaction() throws Exception {
        File binlogFile = new File(BINLOG_FILE_PATH);
        if (!binlogFile.exists()) {
            System.out.println("Binlog file not found at: " + BINLOG_FILE_PATH);
            System.out.println("Please set system property 'test.binlog.path' to point to your binlog file");
            return;
        }
        
        System.out.println("Processing binlog file: " + binlogFile.getAbsolutePath());

        List<LogEvent> events = parseBinlogFile(binlogFile);
        System.out.println("Parsed " + events.size() + " events from binlog file");

        processEventsToTransactions(events);

        validateTransactions();
    }


    /**
     * 处理事件列表，组装成 ApplyTransaction
     */
    private void processEventsToTransactions(List<LogEvent> events) throws InterruptedException {
        int transactionCount = 0;
        boolean process = false;
        Columns columns = null;
        
        for (LogEvent event : events) {
            if (event instanceof ApplierGtidEvent) {
                ApplierGtidEvent gtidEvent = (ApplierGtidEvent) event;
                if ("0a5511ec-1d85-11ef-aca3-b8cef67053e8:2269100498".equals(gtidEvent.getGtid())) {
                    System.out.println("Starting transaction with GTID: " + gtidEvent.getGtid());
                    process = true;
                    mockApplierGroupActivity.doTask(gtidEvent);
                } else {
                    process = false;
                }
                transactionCount++;
            }
            if (event instanceof FetcherRowsEvent && process) {
                ((FetcherRowsEvent<?>) event).setColumns(columns);
                ((FetcherRowsEvent<?>) event).setDirectMemory(directMemory);
                try {
                    ((FetcherRowsEvent<?>) event).tryLoad();
                    cnt += GraphLayout.parseInstance(((FetcherRowsEvent<?>) event).getRows()).totalSize();
                } catch (Exception e) {
                    System.out.println("Error loading rows");
                }
            }
            if (event instanceof ApplierXidEvent && process) {
                mockApplierGroupActivity.doTask((ApplierXidEvent) event);
            } else if (event instanceof TransactionEvent && process) {
                if (event instanceof ApplierTableMapEvent) {
                    columns = Columns.from(((ApplierTableMapEvent) event).getColumns());
                    cnt += GraphLayout.parseInstance(columns).totalSize();
                }
//                System.out.println("Processing transaction event: " + event.getClass().getSimpleName());
                mockApplierGroupActivity.doTask((com.ctrip.framework.drc.fetcher.event.transaction.TransactionEvent) event);
            } else if (process) {
//                System.out.println("Skipping non-transaction event: " + event.getClass().getSimpleName());
            }

            if (process) {
                allEventSize += event.getLogEventHeader().getEventSize();
            }
        }
        
//        System.out.println("Processed " + transactionCount + " transactions");
    }


    private void validateTransactions() throws InterruptedException {
        System.out.println("Transaction validation completed");
        System.out.println("bytes:" + cnt);

        System.out.println(ClassLayout.parseInstance(mockApplierGroupActivity.getTransaction()).toPrintable());
//        System.out.println(GraphLayout.parseInstance(aga.getTransaction()).toFootprint());

        System.gc();
        Thread.sleep(5000);

        GraphLayout layout = GraphLayout.parseInstance(mockApplierGroupActivity.getTransaction());
        System.out.println(layout.toFootprint());

        System.out.println("allEventSize:" + allEventSize);

    }

    /**
     * 从文件读取数据的辅助方法
     */
    private Pair<ByteBuf, Integer> readFile(FileChannel fileChannel, ByteBuffer byteBuffer) throws IOException {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(byteBuffer);
        int readSize = fileChannel.read(byteBuffer);
        byteBuffer.clear();
        return Pair.from(byteBuf, readSize);
    }


    class MockApplierGroupActivity extends ApplierGroupActivity {
        @Override
        public TransactionEvent doTask(TransactionEvent event) throws InterruptedException {
            if (event instanceof BaseBeginEvent) {
                if (current != null) {
                    logger.warn("BeginEvent (Last: UNKNOWN) received without TerminateEvent ahead. - ONLY ON RECONNECT");
                    current.append(getRollbackEvent());
                }
                BaseBeginEvent b = (BaseBeginEvent) event;
                current = getTransaction(b);
            } else {
                current.append(event);
            }

            return null;
        }

        public Transaction getTransaction() {
            return current;
        }
    }


    /**
     * 解析 binlog 文件，返回事件列表
     */
    private List<LogEvent> parseBinlogFile(File file) throws Exception {
        List<LogEvent> events = new ArrayList<>();
        RandomAccessFile raf = null;
        ByteBuffer headBuffer = ByteBuffer.allocateDirect(eventHeaderLengthVersionGt1);
        ByteBuffer bodyBuffer = null;
        int previousBodySize = -1;

        try {
            raf = new RandomAccessFile(file, "r");
            FileChannel fileChannel = raf.getChannel();

            // 跳过文件头
            if (fileChannel.position() == 0) {
                fileChannel.position(LOG_EVENT_START);
            }

            final long endPos = fileChannel.size();
            System.out.println("File size: " + endPos + " bytes");

            while (endPos > fileChannel.position()) {
                // 读取事件头
                Pair<ByteBuf, Integer> headerContent = readFile(fileChannel, headBuffer);
                ByteBuf headerByteBuf = headerContent.getKey();
                int headerSize = headerContent.getValue();

                if (eventHeaderLengthVersionGt1 != headerSize) {
                    System.err.println("Invalid header size: " + headerSize);
                    break;
                }

                // 解析事件类型和大小
                LogEventType eventType = LogEventUtils.parseNextLogEventType(headerByteBuf);
//                System.out.println(eventType);
                long eventSize = LogEventUtils.parseNextLogEventSize(headerByteBuf);

//                System.out.println("Event type: " + eventType + ", size: " + eventSize);

                if (unknown_log_event == eventType) {
                    System.out.println("Reached unknown event, stopping parsing");
                    break;
                }

                // 创建对应的 LogEvent 对象
                LogEvent logEvent = byteBufConverter.getNextEmptyLogEvent(headerByteBuf);
                if (logEvent == null) {
                    // 跳过不支持的事件类型
                    fileChannel.position(fileChannel.position() + eventSize - eventHeaderLengthVersionGt1);
                    continue;
                }

                // 读取事件体
                int bodySize = (int) eventSize - eventHeaderLengthVersionGt1;
                if (bodySize != previousBodySize) {
                    bodyBuffer = ByteBuffer.allocateDirect(bodySize);
                    previousBodySize = bodySize;
                }

                Pair<ByteBuf, Integer> bodyContent = readFile(fileChannel, bodyBuffer);
                ByteBuf bodyByteBuf = bodyContent.getKey();

                // 组装完整事件
                CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer();
                compositeByteBuf.addComponents(true, headerByteBuf, bodyByteBuf);
                logEvent.read(compositeByteBuf);

                events.add(logEvent);
//                System.out.println("Added event: " + logEvent.getClass().getSimpleName());
            }
        } finally {
            if (raf != null) {
                raf.close();
            }
        }

        return events;
    }

} 