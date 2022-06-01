package com.ctrip.framework.drc.core.driver.command.packet.applier;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND;
import com.ctrip.framework.drc.core.driver.command.packet.AbstractCommandPacketTest;
import com.ctrip.framework.drc.core.driver.config.InstanceStatus;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.enums.RowsFilterType;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

import static com.ctrip.framework.drc.core.AllTests.ROW_FILTER_PROPERTIES;

/**
 * Created by mingdongli
 * 2019/9/24 上午8:58.
 */
public class ApplierDumpCommandPacketTest extends AbstractCommandPacketTest {

    private static final String APPLIER_NAME = "test_applier";

    private ApplierDumpCommandPacket packet;

    private String properties = String.format(ROW_FILTER_PROPERTIES, RowsFilterType.TripUid.getName(), "CN");

    @Before
    public void setUp() {
        packet = new ApplierDumpCommandPacket(SERVER_COMMAND.COM_APPLIER_BINLOG_DUMP_GTID.getCode());
        gtidSet = new GtidSet(GTID_SET);
        packet.setApplierName(APPLIER_NAME);
        packet.setGtidSet(gtidSet);

        packet.setApplyMode(ApplyMode.transaction_table.getType());
        packet.setConsumeType(ConsumeType.Slave.getCode());
        packet.setProperties(properties);
    }

    @Test
    public void readAndWrite() throws IOException {
        packet.write(byteBuf);
        ApplierDumpCommandPacket clone = new ApplierDumpCommandPacket(SERVER_COMMAND.COM_APPLIER_BINLOG_DUMP_GTID.getCode());
        clone.read(byteBuf);
        Assert.assertEquals(packet.getApplierName(), clone.getApplierName());
        Assert.assertEquals(packet.getGtidSet(), clone.getGtidSet());
        Assert.assertEquals(packet.getConsumeType(), ConsumeType.Slave.getCode());
        Assert.assertEquals(packet.getApplyMode(), ApplyMode.transaction_table.getType());
        Assert.assertEquals(packet.getProperties(), properties);
    }

    @Test
    public void replicatorBackup() throws IOException {
        packet.setConsumeType(InstanceStatus.INACTIVE.getStatus());
        packet.write(byteBuf);
        ApplierDumpCommandPacket clone = new ApplierDumpCommandPacket(SERVER_COMMAND.COM_APPLIER_BINLOG_DUMP_GTID.getCode());
        clone.read(byteBuf);
        Assert.assertEquals(packet.getApplierName(), clone.getApplierName());
        Assert.assertEquals(packet.getGtidSet(), clone.getGtidSet());
        Assert.assertEquals(packet.getConsumeType(), InstanceStatus.INACTIVE.getStatus());
    }

    @Test
    public void testFilteredDbs() throws IOException {
        Set<String> dbs = Sets.newHashSet();
        dbs.add(RandomStringUtils.randomAlphabetic(10));
        dbs.add(RandomStringUtils.randomAlphabetic(10));
        dbs.add(RandomStringUtils.randomAlphabetic(10));
        ApplierDumpCommandPacket testPackage = new ApplierDumpCommandPacket(RandomStringUtils.randomAlphabetic(10), new GtidSet(""), dbs, "s1.t1");
        ByteBuf tmpByteBuf = PooledByteBufAllocator.DEFAULT.directBuffer();
        testPackage.write(tmpByteBuf);

        ApplierDumpCommandPacket clone = new ApplierDumpCommandPacket(SERVER_COMMAND.COM_APPLIER_BINLOG_DUMP_GTID.getCode());
        clone.read(tmpByteBuf);

        Assert.assertEquals(testPackage.getApplierName(), clone.getApplierName());
        Assert.assertEquals(testPackage.getGtidSet(), clone.getGtidSet());
        Assert.assertEquals(testPackage.getConsumeType(), InstanceStatus.ACTIVE.getStatus());
        Assert.assertEquals(testPackage.getIncludedDbs(), clone.getIncludedDbs());
        Assert.assertEquals(testPackage.getNameFilter(), clone.getNameFilter());
    }
}
