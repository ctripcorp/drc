package com.ctrip.framework.drc.replicator.impl.oubound.handler;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND;
import com.ctrip.framework.drc.core.driver.command.ServerCommandPacket;
import com.ctrip.framework.drc.core.driver.command.packet.applier.ApplierDumpCommandPacket;
import com.ctrip.framework.drc.core.driver.command.packet.client.HeartBeatResponsePacket;
import com.ctrip.framework.drc.core.driver.command.packet.monitor.DelayMonitorCommandPacket;
import com.ctrip.framework.drc.replicator.impl.inbound.AbstractServerTest;
import com.ctrip.xpipe.netty.commands.NettyClient;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by mingdongli
 * 2019/9/22 上午10:40
 */
public class CommandHandlerManagerTest extends AbstractServerTest {

    private CommandHandlerManager commandHandlerManager = new CommandHandlerManager();

    @Mock
    private ApplierRegisterCommandHandler applierRegisterCommandHandler;

    @Mock
    private DelayMonitorCommandHandler delayMonitorCommandHandler;

    @Mock
    private HeartBeatCommandHandler heartBeatCommandHandler;

    @Mock
    private NettyClient nettyClient;

    @Before
    public void setUp() throws Exception {
        super.initMocks();
        when(applierRegisterCommandHandler.getCommandType()).thenReturn(SERVER_COMMAND.COM_APPLIER_BINLOG_DUMP_GTID);
        when(delayMonitorCommandHandler.getCommandType()).thenReturn(SERVER_COMMAND.COM_DELAY_MONITOR);
        when(heartBeatCommandHandler.getCommandType()).thenReturn(SERVER_COMMAND.COM_HEARTBEAT_RESPONSE);
        commandHandlerManager.addHandler(applierRegisterCommandHandler);
        commandHandlerManager.addHandler(delayMonitorCommandHandler);
        commandHandlerManager.addHandler(heartBeatCommandHandler);

        commandHandlerManager.initialize();
    }

    @After
    public void tearDown() throws Exception {
        commandHandlerManager.dispose();
    }

    @Test
    public void handle() throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        commandHandlerManager.handle(nettyClient, getApplierDumpCommandPacket());
        verify(applierRegisterCommandHandler, times(1)).handle(any(ServerCommandPacket.class), any(NettyClient.class));

        commandHandlerManager.handle(nettyClient, getDelayMonitorCommandPacket());
        verify(delayMonitorCommandHandler, times(1)).handle(any(ServerCommandPacket.class), any(NettyClient.class));

        commandHandlerManager.handle(nettyClient, getHeartBeatResponsePacket());
        verify(heartBeatCommandHandler, times(1)).handle(any(ServerCommandPacket.class), any(NettyClient.class));
    }

    private ByteBuf getApplierDumpCommandPacket() throws IOException {
        ApplierDumpCommandPacket dumpCommandPacket = new ApplierDumpCommandPacket(SERVER_COMMAND.COM_APPLIER_BINLOG_DUMP_GTID.getCode());

        dumpCommandPacket.setApplierName("id");
        GtidSet gtidSet = new GtidSet("7b34161a-ceb1-11e9-8bc4-2558475cfacf:1-15");
        dumpCommandPacket.setGtidSet(gtidSet);

        ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.directBuffer();
        dumpCommandPacket.write(byteBuf);

        return byteBuf;
    }

    private ByteBuf getDelayMonitorCommandPacket() throws IOException {
        DelayMonitorCommandPacket monitorCommandPacket = new DelayMonitorCommandPacket("ntgxh", "test_dalcluster", "sha");
        ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.directBuffer();
        monitorCommandPacket.write(byteBuf);

        return byteBuf;
    }

    private ByteBuf getHeartBeatResponsePacket() throws IOException {
        HeartBeatResponsePacket heartBeatResponsePacket = new HeartBeatResponsePacket();
        ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.directBuffer();
        heartBeatResponsePacket.write(byteBuf);

        return byteBuf;
    }
}