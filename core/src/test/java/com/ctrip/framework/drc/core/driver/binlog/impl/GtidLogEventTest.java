package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;
import java.util.UUID;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.gtid_log_event;
import static com.ctrip.framework.drc.core.driver.config.GlobalConfig.LOG_EVENT_IGNORABLE_F;

/**
 * Created by @author zhuYongMing on 2019/9/12.
 */
public class GtidLogEventTest {

    @Test
    public void Testread() {
        final ByteBuf byteBuf = initByteBuf();

        final GtidLogEvent gtidLogEvent = new GtidLogEvent().read(byteBuf);
        if (null == gtidLogEvent) {
            Assert.fail();
        }

        if (null == gtidLogEvent.getLogEventHeader()) {
            Assert.fail();
        }

        // valid decode result
        Assert.assertEquals(gtid_log_event, LogEventType.getLogEventType(gtidLogEvent.getLogEventHeader().getEventType()));
        Assert.assertTrue(!gtidLogEvent.isCommitFlag());
        Assert.assertEquals(new UUID(-6871934785514106391L, -7592793998302186835L).compareTo(gtidLogEvent.getServerUUID()), 0);
        Assert.assertEquals("a0a1fbb8-bdc8-11e9-96a0-fa163e7af2ad:66", gtidLogEvent.getGtid());
        Assert.assertEquals(66, gtidLogEvent.getId());
        Assert.assertEquals(30, gtidLogEvent.getLastCommitted());
        Assert.assertEquals(31, gtidLogEvent.getSequenceNumber());
        Assert.assertEquals("34d9e940", Long.toHexString(gtidLogEvent.getChecksum()));

        Assert.assertEquals(65, byteBuf.readerIndex());
    }


    @Test
    public void testTypeModify() {
        final ByteBuf byteBuf = initByteBuf();
        final GtidLogEvent gtidLogEvent = new GtidLogEvent().read(byteBuf);
        Assert.assertEquals(gtidLogEvent.getLogEventType().getType(), LogEventType.gtid_log_event.getType());
        gtidLogEvent.setEventType(LogEventType.drc_gtid_log_event.getType());
        Assert.assertEquals(gtidLogEvent.getLogEventType().getType(), LogEventType.drc_gtid_log_event.getType());
        Assert.assertEquals(gtidLogEvent.getLogEventHeader().getFlags(), LOG_EVENT_IGNORABLE_F);
        LogEventHeader  logEventHeader = new LogEventHeader().read(gtidLogEvent.getLogEventHeader().getHeaderBuf().slice(0, eventHeaderLengthVersionGt1));
        Assert.assertEquals(logEventHeader.getFlags(), LOG_EVENT_IGNORABLE_F);
        Assert.assertEquals(logEventHeader.getEventType(), LogEventType.drc_gtid_log_event.getType());

        byteBuf.release();
        gtidLogEvent.release();
    }

    @Test
    public void testSetNextTransactionOffsetAndUpdateEventSize() {
        ByteBuf byteBuf = initByteBuf();
        GtidLogEvent gtidLogEvent = new GtidLogEvent().read(byteBuf);
        Random randomNum = new Random();
        int offset =  randomNum.nextInt(1000000);
        gtidLogEvent.setNextTransactionOffsetAndUpdateEventSize(offset);
        Assert.assertEquals(gtidLogEvent.getNextTransactionOffset(), offset);

        ByteBuf headBytebuf = gtidLogEvent.getLogEventHeader().getHeaderBuf();
        headBytebuf.readerIndex(0);
        ByteBuf payloadBytebuf = gtidLogEvent.getPayloadBuf();
        payloadBytebuf.readerIndex(0);

        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer();
        compositeByteBuf.addComponents(true, headBytebuf, payloadBytebuf);
        GtidLogEvent newGtidLogEvent = new GtidLogEvent().read(compositeByteBuf);
        Assert.assertEquals(gtidLogEvent.getNextTransactionOffset(), newGtidLogEvent.getNextTransactionOffset());
        Assert.assertEquals(gtidLogEvent.getChecksum(), newGtidLogEvent.getChecksum());
        Assert.assertEquals(gtidLogEvent.getLogEventType(), newGtidLogEvent.getLogEventType());
        Assert.assertEquals(gtidLogEvent.getServerUUID(), newGtidLogEvent.getServerUUID());
        Assert.assertEquals(gtidLogEvent.getGtid(), newGtidLogEvent.getGtid());
        Assert.assertEquals(gtidLogEvent.getLastCommitted(), newGtidLogEvent.getLastCommitted());
        Assert.assertEquals(gtidLogEvent.getSequenceNumber(), newGtidLogEvent.getSequenceNumber());
        Assert.assertEquals(gtidLogEvent.getId(), newGtidLogEvent.getId());
        Assert.assertEquals(gtidLogEvent.getLogEventHeader().getEventSize(), newGtidLogEvent.getLogEventHeader().getEventSize());
        Assert.assertEquals(gtidLogEvent.getLogEventHeader().getFlags(), newGtidLogEvent.getLogEventHeader().getFlags());
        Assert.assertEquals(gtidLogEvent.getLogEventHeader().getServerId(), newGtidLogEvent.getLogEventHeader().getServerId());
        Assert.assertEquals(gtidLogEvent.getLogEventHeader().getNextEventStartPosition(), newGtidLogEvent.getLogEventHeader().getNextEventStartPosition());
        Assert.assertEquals(gtidLogEvent.getLogEventHeader().getEventTimestamp(), newGtidLogEvent.getLogEventHeader().getEventTimestamp());
    }

    /*
    * # at 11869
    *  #190911 11:35:26 server id 1  end_log_pos 11934 CRC32 0x34d9e940        GTID    last_committed=30       sequence_number=31      rbr_only=yes
    *  !50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED
    *  SET @@SESSION.GTID_NEXT= 'a0a1fbb8-bdc8-11e9-96a0-fa163e7af2ad:66'
    *
    *  7e 6b 78 5d 21 01 00 00  00 41 00 00 00 9e 2e 00
    *  00 00 00 00 a0 a1 fb b8  bd c8 11 e9 96 a0 fa 16
    *  3e 7a f2 ad 42 00 00 00  00 00 00 00 02 1e 00 00
    *  00 00 00 00 00 1f 00 00  00 00 00 00 00 40 e9 d9
    *  34
    */
    private ByteBuf initByteBuf() {
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

    @Test
    public void testConstructor() {
        ByteBuf byteBuf = initByteBuf();
        GtidLogEvent gtidLogEvent = new GtidLogEvent().read(byteBuf);

        GtidLogEvent gtidLogEventClone = new GtidLogEvent(gtidLogEvent.getGtid());
        Assert.assertEquals(gtidLogEventClone.getGtid(), gtidLogEvent.getGtid());
        Assert.assertEquals(gtidLogEventClone.getLogEventType(), gtidLogEvent.getLogEventType());
        Assert.assertEquals(gtidLogEventClone.getLogEventHeader().getEventSize(), gtidLogEvent.getLogEventHeader().getEventSize());

        ByteBuf headerByteBuf = gtidLogEventClone.getLogEventHeader().getHeaderBuf();
        headerByteBuf.readerIndex(0);
        ByteBuf payloadBuf = gtidLogEventClone.getPayloadBuf();
        payloadBuf.readerIndex(0);
        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer();
        compositeByteBuf.addComponents(true, headerByteBuf, payloadBuf);

        GtidLogEvent gtidLogEventFromClone = new GtidLogEvent().read(compositeByteBuf);
        Assert.assertEquals(gtidLogEventFromClone.getGtid(), gtidLogEvent.getGtid());
        Assert.assertEquals(gtidLogEventFromClone.getLogEventType(), gtidLogEvent.getLogEventType());
        Assert.assertEquals(gtidLogEventFromClone.getLogEventHeader().getEventSize(), gtidLogEvent.getLogEventHeader().getEventSize());

        Assert.assertEquals(gtidLogEventFromClone.getGtid(), gtidLogEventClone.getGtid());
        Assert.assertEquals(gtidLogEventFromClone.getSequenceNumber(), gtidLogEventClone.getSequenceNumber());
        Assert.assertEquals(gtidLogEventFromClone.getLastCommitted(), gtidLogEventClone.getLastCommitted());
        Assert.assertEquals(gtidLogEventFromClone.getChecksum(), gtidLogEventClone.getChecksum());
        Assert.assertEquals(gtidLogEventFromClone.getNextTransactionOffset(), gtidLogEventClone.getNextTransactionOffset());
        Assert.assertEquals(gtidLogEventFromClone.getLogEventType(), gtidLogEventClone.getLogEventType());
        Assert.assertEquals(gtidLogEventFromClone.getLogEventHeader().getEventSize(), gtidLogEventClone.getLogEventHeader().getEventSize());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNull() {
        new GtidLogEvent(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithMalformatGtid() {
        new GtidLogEvent("123456789");
    }
}
