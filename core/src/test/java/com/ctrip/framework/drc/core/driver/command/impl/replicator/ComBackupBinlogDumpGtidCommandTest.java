package com.ctrip.framework.drc.core.driver.command.impl.replicator;

import com.ctrip.framework.drc.core.MockTest;
import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.framework.drc.core.driver.binlog.converter.ByteBufConverter;
import com.ctrip.framework.drc.core.driver.binlog.impl.GtidLogEvent;
import com.ctrip.framework.drc.core.driver.command.handler.BackupBinlogDumpGtidClientCommandHandler;
import com.ctrip.framework.drc.core.driver.command.packet.client.BinlogDumpGtidCommandPacket;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @Author limingdong
 * @create 2020/5/11
 */
public class ComBackupBinlogDumpGtidCommandTest extends MockTest {

    private ComBackupBinlogDumpGtidCommand comBackupBinlogDumpGtidCommand;

    private BackupBinlogDumpGtidClientCommandHandler backupBinlogDumpGtidClientCommandHandler;

    @Mock
    private SimpleObjectPool<NettyClient> simpleObjectPool;

    @Mock
    private BinlogDumpGtidCommandPacket binlogDumpGtidCommandPacket;

    @Mock
    private ScheduledExecutorService scheduledExecutorService;

    @Mock
    protected ByteBufConverter byteBufConverter;

    @Mock
    protected LogEventHandler logEventHandler;

    @Before
    public void setUp() throws Exception {
        comBackupBinlogDumpGtidCommand = new ComBackupBinlogDumpGtidCommand(binlogDumpGtidCommandPacket, simpleObjectPool, scheduledExecutorService);
        backupBinlogDumpGtidClientCommandHandler = new BackupBinlogDumpGtidClientCommandHandler(logEventHandler, byteBufConverter);
        comBackupBinlogDumpGtidCommand.addObserver(backupBinlogDumpGtidClientCommandHandler);
    }

    @Test
    public void doReceiveResponse() {
        ByteBuf gtid = getGtidEvent();
        GtidLogEvent gtidLogEvent = new GtidLogEvent().read(gtid);
        ByteBuf headerBuf = gtidLogEvent.getLogEventHeader().getHeaderBuf();
        headerBuf.readerIndex(0);
        ByteBuf payload = gtidLogEvent.getPayloadBuf();
        payload.readerIndex(0);
        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer(headerBuf.readableBytes() + payload.readableBytes());
        compositeByteBuf.addComponents(true, headerBuf, payload);
        when(byteBufConverter.convert(compositeByteBuf)).thenReturn(Lists.newArrayList(gtidLogEvent));
        comBackupBinlogDumpGtidCommand.doReceiveResponse(null, compositeByteBuf);
        verify(logEventHandler, times(1)).onLogEvent(gtidLogEvent, null, null);
        gtid.release();
        gtidLogEvent.release();
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