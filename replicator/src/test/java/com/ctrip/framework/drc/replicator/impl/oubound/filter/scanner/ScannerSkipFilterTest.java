package com.ctrip.framework.drc.replicator.impl.oubound.filter.scanner;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.impl.GtidLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.XidLogEvent;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.AbstractBinlogScanner;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.OutboundLogEventContext;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;
import static org.mockito.Mockito.*;

/**
 * @author yongnian
 * @create 2024/9/20 16:56
 */
public class ScannerSkipFilterTest {

    int fileChannelPos = 321;

    @Test
    public void testNormalCase() throws IOException {
        String uuid = "051bb132-cbdf-4106-8167-91979d5e7052";
        AbstractBinlogScanner binlogScanner = mock(AbstractBinlogScanner.class);
        GtidSet excludedSet = new GtidSet(uuid + ":1-100");
        when(binlogScanner.getGtidSet()).thenReturn(excludedSet);
        ScannerFilterChainContext context = ScannerFilterChainContext.from("test_skip_filter", ConsumeType.Applier, excludedSet, binlogScanner);
        ScannerSkipFilter scannerSkipFilter = new ScannerSkipFilter(context);

        // 1. test excluded gtid
        OutboundLogEventContext value = getGtidContextWithoutOffest(uuid, 100);
        scannerSkipFilter.doFilter(value);
        verify(value.getFileChannel(), times(0)).position(anyLong());
        Assert.assertTrue(value.isSkipEvent());

        value = getTableMapContext();
        scannerSkipFilter.doFilter(value);
        verify(value.getFileChannel(), times(1)).position(tableMapEventSize - eventHeaderLengthVersionGt1);
        Assert.assertTrue(value.isSkipEvent());

        value = getXid();
        scannerSkipFilter.doFilter(value);
        verify(value.getFileChannel(), times(1)).position(xidEventSize - eventHeaderLengthVersionGt1);
        Assert.assertTrue(value.isSkipEvent());

        value = getGtidContextWithoutOffest(uuid, 101);
        scannerSkipFilter.doFilter(value);
        verify(value.getFileChannel(), times(0)).position(anyLong());
        Assert.assertFalse(value.isSkipEvent());

        // 2. test exclude gtid with offset
        value = getGtidContext(uuid, 100);
        scannerSkipFilter.doFilter(value);
        verify(value.getFileChannel(), times(1)).position(gtidEventSize + fileChannelPos + nextTransactionOffset);
        Assert.assertTrue(value.isSkipEvent());

        value = getGtidContext(uuid, 101);
        scannerSkipFilter.doFilter(value);
        verify(value.getFileChannel(), times(0)).position(anyLong());
        Assert.assertFalse(value.isSkipEvent());
    }


    @Test
    public void testNormalCaseMulti() throws IOException {
        String uuid = "051bb132-cbdf-4106-8167-91979d5e7052";
        AbstractBinlogScanner binlogScanner = mock(AbstractBinlogScanner.class);
        GtidSet excludedSet = new GtidSet(uuid + ":1-100");
        when(binlogScanner.getGtidSet()).thenReturn(excludedSet);
        ScannerFilterChainContext context = ScannerFilterChainContext.from("test_skip_filter", ConsumeType.Applier, excludedSet, binlogScanner);
        ScannerSkipFilter scannerSkipFilter = new ScannerSkipFilter(context);

        // 1. test excluded gtid
        OutboundLogEventContext value;

        // 2. test exclude gtid with offset
        for (int i = 1; i <= 100; i++) {
            value = getGtidContext(uuid, i);
            scannerSkipFilter.doFilter(value);
            verify(value.getFileChannel(), times(1)).position(gtidEventSize + fileChannelPos + nextTransactionOffset);
            Assert.assertTrue(value.isSkipEvent());
        }

        value = getGtidContext(uuid, 101);
        scannerSkipFilter.doFilter(value);
        verify(value.getFileChannel(), times(0)).position(anyLong());
        Assert.assertFalse(value.isSkipEvent());
    }

    @Test
    public void testOutOfOrderSupport() throws IOException {
        String uuid = "051bb132-cbdf-4106-8167-91979d5e7052";
        AbstractBinlogScanner binlogScanner = mock(AbstractBinlogScanner.class);
        GtidSet excludedSet = new GtidSet(uuid + ":1-100");
        when(binlogScanner.getGtidSet()).thenReturn(excludedSet);
        ScannerFilterChainContext context = ScannerFilterChainContext.from("test_skip_filter", ConsumeType.Applier, excludedSet, binlogScanner);
        ScannerSkipFilter scannerSkipFilter = new ScannerSkipFilter(context);

        OutboundLogEventContext value;

        // in-order
        for (int i = 101; i < 200; i++) {
            value = getGtidContext(uuid, i);
            scannerSkipFilter.doFilter(value);
            verify(value.getFileChannel(), times(0)).position(anyLong());
            Assert.assertFalse(value.isSkipEvent());

            value = getTableMapContext();
            scannerSkipFilter.doFilter(value);
            Assert.assertFalse(value.isSkipEvent());

            value = getXid();
            scannerSkipFilter.doFilter(value);
            Assert.assertFalse(value.isSkipEvent());
        }
        // out of order: 203 204 202 201 205
        int excluded = 50;
        ArrayList<Integer> ids = Lists.newArrayList(203, 204, excluded, 202, 201, 205);
        for (Integer id : ids) {

            value = getGtidContext(uuid, id);
            scannerSkipFilter.doFilter(value);
            if (id.equals(excluded)) {
                Assert.assertTrue(value.isSkipEvent());
                continue;
            }
            verify(value.getFileChannel(), times(0)).position(anyLong());
            Assert.assertFalse(value.isSkipEvent());

            value = getTableMapContext();
            scannerSkipFilter.doFilter(value);
            Assert.assertFalse(value.isSkipEvent());

            value = getXid();
            scannerSkipFilter.doFilter(value);
            Assert.assertFalse(value.isSkipEvent());
        }


    }

    int xidEventSize = 1805;

    private OutboundLogEventContext getXid() {
        OutboundLogEventContext value = new OutboundLogEventContext();
        value.setEventType(LogEventType.xid_log_event);
        value.setLogEvent(new XidLogEvent());
        value.setFileChannel(mock(FileChannel.class));
        value.setEventSize(xidEventSize);
        value.setFileChannelPos(fileChannelPos);

        Assert.assertFalse(value.isSkipEvent());
        return value;
    }

    int tableMapEventSize = 4185;

    private OutboundLogEventContext getTableMapContext() {
        OutboundLogEventContext value = new OutboundLogEventContext();
        value.setEventType(LogEventType.table_map_log_event);
        value.setLogEvent(new TableMapLogEvent());
        value.setFileChannel(mock(FileChannel.class));
        value.setEventSize(tableMapEventSize);
        value.setFileChannelPos(fileChannelPos);
        Assert.assertFalse(value.isSkipEvent());

        return value;
    }

    int nextTransactionOffset = 85150;
    int gtidEventSize = 4829;

    private OutboundLogEventContext getGtidContext(String uuid, int id) {
        OutboundLogEventContext value = new OutboundLogEventContext();
        value.setEventType(LogEventType.gtid_log_event);
        GtidLogEvent logEvent = new GtidLogEvent(uuid + ":" + id);
        logEvent.setNextTransactionOffsetAndUpdateEventSize(nextTransactionOffset);
        value.setLogEvent(logEvent);
        value.setFileChannel(mock(FileChannel.class));
        value.setEventSize(gtidEventSize);
        value.setFileChannelPos(fileChannelPos);

        Assert.assertFalse(value.isSkipEvent());
        return value;
    }

    private OutboundLogEventContext getGtidContextWithoutOffest(String uuid, int id) {
        OutboundLogEventContext value = new OutboundLogEventContext();
        value.setEventType(LogEventType.gtid_log_event);
        GtidLogEvent logEvent = new GtidLogEvent(uuid + ":" + id);
        value.setLogEvent(logEvent);
        value.setFileChannel(mock(FileChannel.class));
        value.setEventSize(gtidEventSize);
        value.setFileChannelPos(fileChannelPos);

        Assert.assertFalse(value.isSkipEvent());
        return value;
    }
}
