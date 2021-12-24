package com.ctrip.framework.drc.fetcher.activity.replicator.handler.command;

import com.ctrip.framework.drc.core.driver.binlog.LogEventCallBack;
import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.framework.drc.core.driver.binlog.converter.ByteBufConverter;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcErrorLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.GtidLogEvent;
import com.ctrip.framework.drc.core.driver.command.impl.replicator.ComBinlogDumpGtidCommand;
import com.ctrip.framework.drc.core.driver.command.packet.applier.ApplierDumpCommandPacket;
import com.ctrip.framework.drc.core.exception.LogEventException;
import com.ctrip.framework.drc.fetcher.MockTest;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.BorrowObjectException;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static com.ctrip.framework.drc.core.driver.command.packet.ResultCode.HANDLE_FAIL;

/**
 * @Author limingdong
 * @create 2021/3/24
 */
public class FetcherBinlogDumpGtidCommandHandlerTest extends MockTest {

    private FetcherBinlogDumpGtidCommandHandler binlogDumpGtidClientCommandHandler;

    @Mock
    private ByteBufConverter byteBufConverter;

    @Mock
    private LogEventHandler logEventHandler;

    @Mock
    private ApplierDumpCommandPacket binlogDumpGtidCommandPacket;

    @Mock
    private SimpleObjectPool<NettyClient> simpleObjectPool;

    @Mock
    private ComBinlogDumpGtidCommand dumpGtidCommand;

    @Mock
    private Channel channel;

    private LogEventCallBack logEventCallBack;

    @Before
    public void setUp() throws Exception {
        super.initMocks();
        binlogDumpGtidClientCommandHandler = new FetcherBinlogDumpGtidCommandHandler(logEventHandler, byteBufConverter);
        logEventCallBack = binlogDumpGtidClientCommandHandler.getLogEventCallBack(channel);
    }

    @Test
    public void testHandle() throws BorrowObjectException {
        binlogDumpGtidClientCommandHandler.handle(binlogDumpGtidCommandPacket, simpleObjectPool);
        verify(simpleObjectPool, times(1)).borrowObject();
    }

    @Test(expected = LogEventException.class)
    public void testUpdateDrcErrorLogEvent() {
        DrcErrorLogEvent errorLogEvent = new DrcErrorLogEvent(HANDLE_FAIL.getCode(), HANDLE_FAIL.getMessage());
        ByteBuf headerBuf = errorLogEvent.getLogEventHeader().getHeaderBuf();
        headerBuf.readerIndex(0);
        ByteBuf payload = errorLogEvent.getPayloadBuf();
        payload.readerIndex(0);
        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer(headerBuf.readableBytes() + payload.readableBytes());
        compositeByteBuf.addComponents(true, headerBuf, payload);
        when(byteBufConverter.convert(compositeByteBuf)).thenReturn(Lists.newArrayList(errorLogEvent));
        binlogDumpGtidClientCommandHandler.update(Pair.from(compositeByteBuf, channel), dumpGtidCommand);
        verify(logEventHandler, times(0)).onLogEvent(anyObject(), logEventCallBack, null);
        compositeByteBuf.release();
    }

    @Test
    public void testUpdate() {
        ByteBuf gtid = getGtidEvent();
        GtidLogEvent gtidLogEvent = new GtidLogEvent().read(gtid);
        ByteBuf headerBuf = gtidLogEvent.getLogEventHeader().getHeaderBuf();
        headerBuf.readerIndex(0);
        ByteBuf payload = gtidLogEvent.getPayloadBuf();
        payload.readerIndex(0);
        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer(headerBuf.readableBytes() + payload.readableBytes());
        compositeByteBuf.addComponents(true, headerBuf, payload);
        when(byteBufConverter.convert(compositeByteBuf)).thenReturn(Lists.newArrayList(gtidLogEvent));
        binlogDumpGtidClientCommandHandler.update(Pair.from(compositeByteBuf, channel), dumpGtidCommand);
        verify(logEventHandler, times(1)).onLogEvent(gtidLogEvent, logEventCallBack, null);
        gtid.release();
        gtid.release();
    }

    protected ByteBuf getGtidEvent() {
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
}