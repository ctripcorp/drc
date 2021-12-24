package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.MockTest;
import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.server.common.Filter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

/**
 * @Author limingdong
 * @create 2020/6/16
 */
public class TransactionEventTest extends MockTest {

    @Mock
    private IoCache ioCache;

    @Mock
    private GtidLogEvent gtidLogEvent;

    @Mock
    private LogEventHeader logEventHeader;

    @Mock
    private ByteBuf headerBuf;

    @Mock
    private ByteBuf payloadBuf;

    @Mock
    private XidLogEvent xidLogEvent;

    @Mock
    private DrcDdlLogEvent drcDdlLogEvent;

    private TransactionEvent transactionEvent = new TransactionEvent();

    @Before
    public void setUp() throws Exception {
        super.initMocks();
        when(gtidLogEvent.getLogEventHeader()).thenReturn(logEventHeader);
        when(xidLogEvent.getLogEventHeader()).thenReturn(logEventHeader);
        when(logEventHeader.getHeaderBuf()).thenReturn(headerBuf);
        when(gtidLogEvent.getPayloadBuf()).thenReturn(payloadBuf);
        when(xidLogEvent.getPayloadBuf()).thenReturn(payloadBuf);
    }

    @Test
    public void writeAndRelease() {
        transactionEvent.addLogEvent(gtidLogEvent);
        transactionEvent.addLogEvent(xidLogEvent);
        transactionEvent.write(ioCache);

        transactionEvent.release();
        verify(gtidLogEvent, times(1)).release();
        verify(xidLogEvent, times(1)).release();
    }

    @Test
    public void testFalse() {
        //empty
        TransactionEvent transactionEvent = new TransactionEvent();
        transactionEvent.write(ioCache);

        //first not gtid
        transactionEvent = new TransactionEvent();
        ByteBuf xidByteBuf = getXidEvent();
        XidLogEvent xidLogEvent = new XidLogEvent().read(xidByteBuf);
        transactionEvent.addLogEvent(xidLogEvent);
        xidByteBuf.release();
        transactionEvent.release();

        //last not xid
        transactionEvent = new TransactionEvent();
        ByteBuf updateRowsEventByteBuf = getRowsEventByteBuf();
        UpdateRowsEvent updateRowsEvent = new UpdateRowsEvent().read(updateRowsEventByteBuf);
        transactionEvent.addLogEvent(updateRowsEvent);
        updateRowsEventByteBuf.release();
        transactionEvent.release();

        //drc_ddl_log_event
        when(drcDdlLogEvent.getLogEventType()).thenReturn(LogEventType.drc_ddl_log_event);
        when(drcDdlLogEvent.getLogEventHeader()).thenReturn(logEventHeader);
        when(drcDdlLogEvent.getPayloadBuf()).thenReturn(headerBuf);
        transactionEvent = new TransactionEvent();
        ByteBuf gtidByteBuf = getGtidEvent();
        GtidLogEvent gtidLogEvent = new GtidLogEvent().read(gtidByteBuf);
        long nextEventSize = gtidLogEvent.getNextTransactionOffset();

        xidByteBuf = getXidEvent();
        xidLogEvent = new XidLogEvent().read(xidByteBuf);

        transactionEvent.addLogEvent(gtidLogEvent);
        transactionEvent.addLogEvent(drcDdlLogEvent);
        transactionEvent.addLogEvent(xidLogEvent);
        transactionEvent.write(ioCache);

        Assert.assertEquals(nextEventSize, gtidLogEvent.getNextTransactionOffset());

        gtidByteBuf.release();
        xidByteBuf.release();
        transactionEvent.release();

    }

    @Test
    public void testOneGtid() {
        ByteBuf gtidByteBuf = getGtidEvent();
        GtidLogEvent gtidLogEvent = new GtidLogEvent().read(gtidByteBuf);
        TransactionEvent transactionEvent = new TransactionEvent();

        transactionEvent.addLogEvent(gtidLogEvent);
        long nextEventSize = gtidLogEvent.getNextTransactionOffset();
        transactionEvent.write(ioCache);
        Assert.assertEquals(nextEventSize, gtidLogEvent.getNextTransactionOffset());

        gtidByteBuf.release();
        transactionEvent.release();
    }

    @Test
    public void testOneGtidAndXid() {
        ByteBuf gtidByteBuf = getGtidEvent();
        GtidLogEvent gtidLogEvent = new GtidLogEvent().read(gtidByteBuf);
        ByteBuf xidByteBuf = getXidEvent();
        XidLogEvent xidLogEvent = new XidLogEvent().read(xidByteBuf);
        TransactionEvent transactionEvent = new TransactionEvent();

        transactionEvent.addLogEvent(gtidLogEvent);
        transactionEvent.addLogEvent(xidLogEvent);
        long nextEventSize = gtidLogEvent.getNextTransactionOffset();
        transactionEvent.write(ioCache);
        Assert.assertEquals(nextEventSize, gtidLogEvent.getNextTransactionOffset());

        gtidByteBuf.release();
        transactionEvent.release();
    }

    private ByteBuf getGtidEvent() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(65);
        byte[] bytes = new byte[] {
                (byte) 0x7e, (byte) 0x6b, (byte) 0x78, (byte) 0x5d, (byte) 0x21, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x41, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x9e, (byte) 0x2e, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xa0, (byte) 0xa1, (byte) 0xfb, (byte) 0xb8,
                (byte) 0xbd, (byte) 0xc8, (byte) 0x11, (byte) 0xe9, (byte) 0x96, (byte) 0xa0, (byte) 0xfa, (byte) 0x16,

                (byte) 0x3e, (byte) 0x7a, (byte) 0xf2, (byte) 0xad, (byte) 0x42, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x02, (byte) 0x1e, (byte) 0x00, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x1f, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x40, (byte) 0xe9, (byte) 0xd9,

                (byte) 0x34
        };
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    private ByteBuf getXidEvent() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(35);
        byte[] bytes = new byte[] {
                (byte) 0x7e, (byte) 0x6b, (byte) 0x78, (byte) 0x5d, (byte) 0x10, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x1f, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xf5, (byte) 0x2f, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x44, (byte) 0x04, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x82, (byte) 0xe5, (byte) 0xc7, (byte) 0x3a
        };
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    private ByteBuf getRowsEventByteBuf() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(49);
        byte[] bytes = new byte[]{
                (byte) 0x91, (byte) 0x5d, (byte) 0x7e, (byte) 0x5d, (byte) 0x1e, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x31, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xbd, (byte) 0x06, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x7b, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x02, (byte) 0x00, (byte) 0x04, (byte) 0x09, (byte) 0xfc,

                (byte) 0x09, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x07, (byte) 0x00, (byte) 0x76, (byte) 0x61,
                (byte) 0x72, (byte) 0x63, (byte) 0x68, (byte) 0x61, (byte) 0x72, (byte) 0xb8, (byte) 0x74, (byte) 0xe8,

                (byte) 0x80
        };
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }
}
