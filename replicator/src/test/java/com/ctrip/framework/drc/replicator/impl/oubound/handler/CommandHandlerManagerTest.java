package com.ctrip.framework.drc.replicator.impl.oubound.handler;

import com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND;
import com.ctrip.framework.drc.core.driver.command.packet.applier.ApplierDumpCommandPacket;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.replicator.impl.inbound.AbstractServerTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by mingdongli
 * 2019/9/22 上午10:40
 */
public class CommandHandlerManagerTest extends AbstractServerTest {

    @Test
    public void handle() throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {

        CommandHandlerManager handlerManager = new CommandHandlerManager();
        handlerManager.addHandler(new ApplierRegisterCommandHandler(null, null, null));
        ApplierDumpCommandPacket dumpCommandPacket = new ApplierDumpCommandPacket(SERVER_COMMAND.COM_APPLIER_BINLOG_DUMP_GTID.getCode());

        dumpCommandPacket.setApplierName("id");
        GtidSet gtidSet = new GtidSet("7b34161a-ceb1-11e9-8bc4-2558475cfacf:1-15");
        dumpCommandPacket.setGtidSet(gtidSet);

        ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.directBuffer();
        dumpCommandPacket.write(byteBuf);

        handlerManager.handle(null, byteBuf);

    }
}