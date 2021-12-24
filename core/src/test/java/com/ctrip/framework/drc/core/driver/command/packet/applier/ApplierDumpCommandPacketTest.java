package com.ctrip.framework.drc.core.driver.command.packet.applier;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND;
import com.ctrip.framework.drc.core.driver.command.packet.AbstractCommandPacketTest;
import com.ctrip.framework.drc.core.driver.config.InstanceStatus;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

/**
 * Created by mingdongli
 * 2019/9/24 上午8:58.
 */
public class ApplierDumpCommandPacketTest extends AbstractCommandPacketTest {

    private static final String APPLIER_NAME = "test_applier";

    private ApplierDumpCommandPacket packet;

    @Before
    public void setUp() {
        packet = new ApplierDumpCommandPacket(SERVER_COMMAND.COM_APPLIER_BINLOG_DUMP_GTID.getCode());
        gtidSet = new GtidSet(GTID_SET);
        packet.setApplierName(APPLIER_NAME);
        packet.setGtidSet(gtidSet);
    }

    @Test
    public void readAndWrite() throws IOException {
        packet.write(byteBuf);
        ApplierDumpCommandPacket clone = new ApplierDumpCommandPacket(SERVER_COMMAND.COM_APPLIER_BINLOG_DUMP_GTID.getCode());
        clone.read(byteBuf);
        Assert.assertEquals(packet.getApplierName(), clone.getApplierName());
        Assert.assertEquals(packet.getGtidSet(), clone.getGtidSet());
        Assert.assertEquals(packet.getReplicatroBackup(), InstanceStatus.ACTIVE.getStatus());
    }

    @Test
    public void replicatorBackup() throws IOException {
        packet.setReplicatroBackup(InstanceStatus.INACTIVE.getStatus());
        packet.write(byteBuf);
        ApplierDumpCommandPacket clone = new ApplierDumpCommandPacket(SERVER_COMMAND.COM_APPLIER_BINLOG_DUMP_GTID.getCode());
        clone.read(byteBuf);
        Assert.assertEquals(packet.getApplierName(), clone.getApplierName());
        Assert.assertEquals(packet.getGtidSet(), clone.getGtidSet());
        Assert.assertEquals(packet.getReplicatroBackup(), InstanceStatus.INACTIVE.getStatus());
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
        Assert.assertEquals(testPackage.getReplicatroBackup(), InstanceStatus.ACTIVE.getStatus());
        Assert.assertEquals(testPackage.getIncludedDbs(), clone.getIncludedDbs());
        Assert.assertEquals(testPackage.getNameFilter(), clone.getNameFilter());
    }

    @Test
    public void testLong() {
        System.out.println((long) ((-128 & 0xff) << 24));
        System.out.println(((long) ((-128 & 0xff) << 24)) | 11476371);
        System.out.println(( (long)(-128 & 0xff) << 24));

        System.out.println(Long.MAX_VALUE);
        System.out.println(Long.MIN_VALUE);
    }

}
