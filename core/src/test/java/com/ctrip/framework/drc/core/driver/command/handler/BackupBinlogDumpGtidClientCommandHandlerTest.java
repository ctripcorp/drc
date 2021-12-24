package com.ctrip.framework.drc.core.driver.command.handler;

import com.ctrip.framework.drc.core.MockTest;
import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.framework.drc.core.driver.binlog.converter.ByteBufConverter;
import com.ctrip.framework.drc.core.driver.command.packet.client.BinlogDumpGtidCommandPacket;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.BorrowObjectException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

/**
 * @Author limingdong
 * @create 2020/5/11
 */
public class BackupBinlogDumpGtidClientCommandHandlerTest extends MockTest {

    private BackupBinlogDumpGtidClientCommandHandler backupBinlogDumpGtidClientCommandHandler;

    @Mock
    protected ByteBufConverter byteBufConverter;

    @Mock
    protected LogEventHandler logEventHandler;

    @Mock
    protected BinlogDumpGtidCommandPacket binlogDumpGtidCommandPacket;

    @Mock
    protected SimpleObjectPool<NettyClient> simpleObjectPool;

    @Before
    public void setUp() throws Exception {
        super.initMocks();
        backupBinlogDumpGtidClientCommandHandler = new BackupBinlogDumpGtidClientCommandHandler(logEventHandler, byteBufConverter);
    }

    @Test
    public void handle() throws BorrowObjectException {
        backupBinlogDumpGtidClientCommandHandler.handle(binlogDumpGtidCommandPacket, simpleObjectPool);
        verify(simpleObjectPool, times(1)).borrowObject();
    }
}