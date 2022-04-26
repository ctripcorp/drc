package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.replicator.store.AbstractTransactionTest;
import io.netty.buffer.ByteBuf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;
import static com.ctrip.framework.drc.replicator.store.manager.file.DefaultFileManager.LOG_PATH;

/**
 * @Author limingdong
 * @create 2022/4/23
 */
public class TableFilterTest extends AbstractTransactionTest {

    private static final long table_id = 1114;

    private TableFilter tableFilter = new TableFilter();

    private File logDir = new File(LOG_PATH + "tmp_ut");

    private File rbinlog;

    private OutboundLogEventContext outboundLogEventContext;

    private LogEventType logEventType;

    private FileChannel fileChannel;

    @Before
    public void setUp() throws Exception {
        if (!logDir.exists()) {
            boolean created = logDir.mkdirs();
            logger.info("create {} {}", logDir, created);
        }

        rbinlog = new File(logDir + "/ut.log");

        RandomAccessFile raf = new RandomAccessFile(rbinlog, "rw");
        fileChannel = raf.getChannel();
    }

    @After
    public void tearDown() throws Exception {
        fileChannel.close();
        rbinlog.delete();
    }

    @Test
    public void doFilter() throws IOException {
        // drc_table_map_log_event
        TableMapLogEvent tableMapLogEvent = getDrcTableMapLogEvent();
        String tableName = tableMapLogEvent.getSchemaNameDotTableName();
        ByteBuf byteBuf = tableMapLogEvent.getLogEventHeader().getHeaderBuf();
        int endIndex = byteBuf.writerIndex();
        ByteBuffer byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);

        byteBuf = tableMapLogEvent.getPayloadBuf();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);

        long previousPosition = 0;
        long currentPosition = fileChannel.position();
        fileChannel.position(previousPosition + eventHeaderLengthVersionGt1);

        logEventType = LogEventType.drc_table_map_log_event;
        outboundLogEventContext = new OutboundLogEventContext(fileChannel, previousPosition + eventHeaderLengthVersionGt1, logEventType, currentPosition - previousPosition, "");
        boolean skip = tableFilter.doFilter(outboundLogEventContext);
        Assert.assertTrue(skip);
        Assert.assertEquals(1, tableFilter.getDrcTableMap().size());

        fileChannel.position(currentPosition);
        previousPosition = currentPosition;

        // table_map_log_event
        byteBuf = getCharsetTypeTableMapEvent();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);

        currentPosition = fileChannel.position();
        fileChannel.position(previousPosition + eventHeaderLengthVersionGt1);

        logEventType = LogEventType.table_map_log_event;
        outboundLogEventContext = new OutboundLogEventContext(fileChannel, previousPosition + eventHeaderLengthVersionGt1, logEventType, currentPosition - previousPosition, "");
        skip = tableFilter.doFilter(outboundLogEventContext);
        Assert.assertTrue(skip);
        Assert.assertEquals(1, tableFilter.getTableMapWithinTransaction().size());

        fileChannel.position(currentPosition);
        previousPosition = currentPosition;

        // rows_log_event
        byteBuf = getMinimalRowsEventByteBuf();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);

        currentPosition = fileChannel.position();
        fileChannel.position(previousPosition + eventHeaderLengthVersionGt1);

        logEventType = LogEventType.write_rows_event_v2;
        outboundLogEventContext = new OutboundLogEventContext(fileChannel, previousPosition + eventHeaderLengthVersionGt1, logEventType, currentPosition - previousPosition, "");
        skip = tableFilter.doFilter(outboundLogEventContext);
        Assert.assertFalse(skip);
        Assert.assertNotNull(outboundLogEventContext.getDrcTableMap(tableName));
        Assert.assertNotNull(outboundLogEventContext.getTableMapWithinTransaction(table_id));

        fileChannel.position(currentPosition);
        previousPosition = currentPosition;

        // xid_log_event
        byteBuf = getXidEvent();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);

        currentPosition = fileChannel.position();
        fileChannel.position(previousPosition + eventHeaderLengthVersionGt1);

        logEventType = LogEventType.xid_log_event;
        outboundLogEventContext = new OutboundLogEventContext(fileChannel, previousPosition + eventHeaderLengthVersionGt1, logEventType, currentPosition - previousPosition, "");
        skip = tableFilter.doFilter(outboundLogEventContext);
        Assert.assertTrue(skip);

        Assert.assertNull(outboundLogEventContext.getDrcTableMap(tableName));
        Assert.assertNull(outboundLogEventContext.getTableMapWithinTransaction(table_id));
        Assert.assertEquals(1, tableFilter.getDrcTableMap().size());
        Assert.assertEquals(0, tableFilter.getTableMapWithinTransaction().size());  // clear

    }
}