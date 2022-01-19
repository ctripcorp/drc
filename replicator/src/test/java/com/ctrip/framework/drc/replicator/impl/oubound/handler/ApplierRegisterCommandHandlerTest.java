package com.ctrip.framework.drc.replicator.impl.oubound.handler;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidManager;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcDdlLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.GtidLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.manager.SchemaManager;
import com.ctrip.framework.drc.core.driver.command.packet.applier.ApplierDumpCommandPacket;
import com.ctrip.framework.drc.core.driver.config.InstanceStatus;
import com.ctrip.framework.drc.core.monitor.kpi.OutboundMonitorReport;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.replicator.store.AbstractTransactionTest;
import com.ctrip.framework.drc.replicator.store.manager.file.DefaultFileManager;
import com.ctrip.framework.drc.replicator.store.manager.gtid.DefaultGtidManager;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.utils.Gate;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.DefaultFileRegion;
import io.netty.util.Attribute;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Set;

/**
 * @Author limingdong
 * @create 2020/6/22
 */
@FixMethodOrder(value = MethodSorters.NAME_ASCENDING)
public class ApplierRegisterCommandHandlerTest extends AbstractTransactionTest {

    @InjectMocks
    private ApplierRegisterCommandHandler applierRegisterCommandHandler;

    @Mock
    private Channel channel;

    @Mock
    private ApplierDumpCommandPacket dumpCommandPacket;

    @Mock
    private OutboundMonitorReport outboundMonitorReport;

    @Mock
    private NettyClient nettyClient;

    @Mock
    private ChannelFuture channelFuture;

    @Mock
    private SchemaManager schemaManager;

    @Mock
    private Attribute<Gate> attribute;

    @Mock
    private Gate gate;

    private static final String APPLIER_NAME = "ut_applier_name";

    private static final String LOCAL_HOST = "127.0.0.1";

    private static final String LAST_FILE_NAME = "rbinlog.0000000004";

    //send file 1 and 4
    private static final GtidSet EXCLUDED_GTID = new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-1510:1512-2000");

    private InetSocketAddress socketAddress = new InetSocketAddress(LOCAL_HOST, 8080);

    private GtidManager gtidManager;

    @Before
    public void setUp() throws Exception {
        fileManager = new DefaultFileManager(schemaManager, APPLIER_NAME);
        gtidManager = new DefaultGtidManager(fileManager);
        fileManager.initialize();
        fileManager.start();
        fileManager.setGtidManager(gtidManager);

        gtidManager.setUuids(Sets.newHashSet("c372080a-1804-11ea-8add-98039bbedf9c"));

        applierRegisterCommandHandler = new ApplierRegisterCommandHandler(gtidManager, fileManager, outboundMonitorReport);

        createFiles();
    }

    @After
    public void tearDown() throws Exception {
        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);
        fileManager.stop();
        fileManager.dispose();
    }

    @Test
    public void test_01_SkipFiles() throws InterruptedException {
        when(dumpCommandPacket.getApplierName()).thenReturn(APPLIER_NAME);
        when(nettyClient.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(socketAddress);
        when(channel.closeFuture()).thenReturn(channelFuture);
        when(dumpCommandPacket.getReplicatroBackup()).thenReturn(InstanceStatus.INACTIVE.getStatus());
        when(dumpCommandPacket.getGtidSet()).thenReturn(EXCLUDED_GTID);
        when(channel.attr(ReplicatorMasterHandler.KEY_CLIENT)).thenReturn(attribute);
        when(attribute.get()).thenReturn(gate);

        applierRegisterCommandHandler.handle(dumpCommandPacket, nettyClient);

        while (!LAST_FILE_NAME.equalsIgnoreCase(fileManager.getCurrentLogFile().getName())) {
            Thread.sleep(100);
        }
        Thread.sleep(300);

        verify(channel, times(2 * 2 + 1 + 2)).writeAndFlush(any(DefaultFileRegion.class));  //gtid and drc_uuid_log in last two files, and format_log、 previous_log in last file, switch file will send empty msg
        verify(outboundMonitorReport, times(2)).addOutboundGtid(anyString(), anyString());
        verify(outboundMonitorReport, times(2)).addOneCount();
        verify(outboundMonitorReport, times(2 * 2 + 2)).addSize(any(Long.class)); // +2 for drc_uuid_log
    }

    @Test
    public void test_02_DrcTableMapLogEvent() throws Exception {
        when(dumpCommandPacket.getApplierName()).thenReturn(APPLIER_NAME);
        when(nettyClient.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(socketAddress);
        when(channel.closeFuture()).thenReturn(channelFuture);
        when(dumpCommandPacket.getReplicatroBackup()).thenReturn(InstanceStatus.INACTIVE.getStatus());
        when(dumpCommandPacket.getGtidSet()).thenReturn(EXCLUDED_GTID);
        when(channel.attr(ReplicatorMasterHandler.KEY_CLIENT)).thenReturn(attribute);
        when(attribute.get()).thenReturn(gate);

        createDrcTableMapLogEventFiles();
        applierRegisterCommandHandler.handle(dumpCommandPacket, nettyClient);
        Thread.sleep(250);

        verify(channel, times( 1 + 1)).writeAndFlush(any(DefaultFileRegion.class));  //xid and drc_uuid_log
        verify(outboundMonitorReport, times(0)).addOutboundGtid(anyString(), anyString());
        verify(outboundMonitorReport, times(0)).addOneCount();
    }

    @Test
    public void test_03_DrcDdlLogEvent() throws Exception {
        when(dumpCommandPacket.getApplierName()).thenReturn(APPLIER_NAME);
        when(nettyClient.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(socketAddress);
        when(channel.closeFuture()).thenReturn(channelFuture);
        when(dumpCommandPacket.getReplicatroBackup()).thenReturn(InstanceStatus.INACTIVE.getStatus());
        when(dumpCommandPacket.getGtidSet()).thenReturn(EXCLUDED_GTID);
        when(channel.attr(ReplicatorMasterHandler.KEY_CLIENT)).thenReturn(attribute);
        when(attribute.get()).thenReturn(gate);

        createDrcDdlLogEventFiles();
        applierRegisterCommandHandler.handle(dumpCommandPacket, nettyClient);
        Thread.sleep(250);

        verify(channel, times( 3)).writeAndFlush(any(DefaultFileRegion.class));  //ddl、drc table map、 drc_uuid_log
        verify(outboundMonitorReport, times(0)).addOutboundGtid(anyString(), anyString());
        verify(outboundMonitorReport, times(0)).addOneCount();
    }

    @Test
    public void test_04_DrcIndexLogEvent() throws Exception {
        when(dumpCommandPacket.getApplierName()).thenReturn(APPLIER_NAME);
        when(nettyClient.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(socketAddress);
        when(channel.closeFuture()).thenReturn(channelFuture);
        when(dumpCommandPacket.getReplicatroBackup()).thenReturn(InstanceStatus.INACTIVE.getStatus());
        GtidSet gtidSet = new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-3500");
        when(dumpCommandPacket.getGtidSet()).thenReturn(gtidSet);
        when(channel.attr(ReplicatorMasterHandler.KEY_CLIENT)).thenReturn(attribute);
        when(attribute.get()).thenReturn(gate);

        int transactionSize = createDrcIndexLogEventFiles();
        applierRegisterCommandHandler.handle(dumpCommandPacket, nettyClient);
        Thread.sleep(250);

        verify(channel, Mockito.atLeast( 2 + transactionSize * 4)).writeAndFlush(any(DefaultFileRegion.class));  //ddl、drc table map
    }

    @Test
    public void test_04_DrcIndexLogEventWithDdl() throws Exception {
        when(dumpCommandPacket.getApplierName()).thenReturn(APPLIER_NAME);
        when(nettyClient.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(socketAddress);
        when(channel.closeFuture()).thenReturn(channelFuture);
        when(dumpCommandPacket.getReplicatroBackup()).thenReturn(InstanceStatus.INACTIVE.getStatus());
        GtidSet gtidSet = new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-3500");
        when(dumpCommandPacket.getGtidSet()).thenReturn(gtidSet);
        when(channel.attr(ReplicatorMasterHandler.KEY_CLIENT)).thenReturn(attribute);
        when(attribute.get()).thenReturn(gate);

        int transactionSize = createDrcIndexLogEventFilesWithDdl();
        applierRegisterCommandHandler.handle(dumpCommandPacket, nettyClient);
        Thread.sleep(250);

        verify(channel, Mockito.atLeast( 2 + transactionSize * 4)).writeAndFlush(any(DefaultFileRegion.class));  //ddl、drc table map
    }

    @Test
    public void test_05_DrcIndexLogEventWithGtidsetNotInclude() throws Exception {
        when(dumpCommandPacket.getApplierName()).thenReturn(APPLIER_NAME);
        when(nettyClient.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(socketAddress);
        when(channel.closeFuture()).thenReturn(channelFuture);
        when(dumpCommandPacket.getReplicatroBackup()).thenReturn(InstanceStatus.INACTIVE.getStatus());
        GtidSet gtidSet = new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-2001:2003-3500");
        when(dumpCommandPacket.getGtidSet()).thenReturn(gtidSet);
        when(channel.attr(ReplicatorMasterHandler.KEY_CLIENT)).thenReturn(attribute);
        when(attribute.get()).thenReturn(gate);

        int transactionSize = createDrcIndexLogEventFiles();
        applierRegisterCommandHandler.handle(dumpCommandPacket, nettyClient);
        Thread.sleep(250);

        verify(channel, Mockito.atLeast( 2 + transactionSize * 4)).writeAndFlush(any(DefaultFileRegion.class));  //ddl、drc table map
    }

    @Test
    public void test_06_DrcIndexLogEventWithRealGtidsetInclude() throws Exception {
        when(dumpCommandPacket.getApplierName()).thenReturn(APPLIER_NAME);
        when(nettyClient.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(socketAddress);
        when(channel.closeFuture()).thenReturn(channelFuture);
        when(dumpCommandPacket.getReplicatroBackup()).thenReturn(InstanceStatus.INACTIVE.getStatus());
        int maxGtidId = testIndexFileWithDiffGtidSetAndDdl();
        GtidSet gtidSet = new GtidSet(UUID_STRING + ":1-" + maxGtidId);
        when(dumpCommandPacket.getGtidSet()).thenReturn(gtidSet);
        when(channel.attr(ReplicatorMasterHandler.KEY_CLIENT)).thenReturn(attribute);
        when(attribute.get()).thenReturn(gate);

        applierRegisterCommandHandler.handle(dumpCommandPacket, nettyClient);
        Thread.sleep(250);

        verify(channel, Mockito.times( 2/*ddl with 2 events, drc_ddl and drc_talbe_map*/ + 1 /*previous_gtid_log_event in the mid file*/ + 1 /*drc_uuid_log_event in the mid file*/ + 1 /*empty msg to close file channel*/)).writeAndFlush(any(DefaultFileRegion.class));
    }

    @Test
    public void test_07_DrcIndexLogEventWithRealGtidsetNotInclude() throws Exception {
        when(dumpCommandPacket.getApplierName()).thenReturn(APPLIER_NAME);
        when(nettyClient.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(socketAddress);
        when(channel.closeFuture()).thenReturn(channelFuture);
        when(dumpCommandPacket.getReplicatroBackup()).thenReturn(InstanceStatus.INACTIVE.getStatus());
        int maxGtidId = testIndexFileWithDiffGtidSetAndDdl();
        int ddlId = (maxGtidId - 1) / 2;
        GtidSet gtidSet = new GtidSet(UUID_STRING + ":" + ddlId);
        when(dumpCommandPacket.getGtidSet()).thenReturn(gtidSet);
        when(channel.attr(ReplicatorMasterHandler.KEY_CLIENT)).thenReturn(attribute);
        when(attribute.get()).thenReturn(gate);

        applierRegisterCommandHandler.handle(dumpCommandPacket, nettyClient);
        Thread.sleep(500);

        verify(channel, Mockito.times( 2/*ddl with 2 events, drc_ddl and drc_talbe_map*/ + 1 /*previous_gtid_log_event in the mid file*/ + 1 /*drc_uuid_log_event in the mid file*/ + (maxGtidId - 1) * 4 /*transaction size*/ + 1 /*empty msg to close file channel*/)).writeAndFlush(any(DefaultFileRegion.class));
    }

    @Test
    public void test_08_DrcIndexLogEventWithRealGtidsetWithGapBeforeDdl() throws Exception {
        when(dumpCommandPacket.getApplierName()).thenReturn(APPLIER_NAME);
        when(nettyClient.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(socketAddress);
        when(channel.closeFuture()).thenReturn(channelFuture);
        when(dumpCommandPacket.getReplicatroBackup()).thenReturn(InstanceStatus.INACTIVE.getStatus());
        int maxGtidId = testIndexFileWithDiffGtidSetAndDdl();
        int ddlId = (maxGtidId - 1) / 2;
        GtidSet gtidSet = new GtidSet(UUID_STRING + ":1-" + (ddlId - 3) + ":" + (ddlId - 1) + "-" + maxGtidId);
        when(dumpCommandPacket.getGtidSet()).thenReturn(gtidSet);
        when(channel.attr(ReplicatorMasterHandler.KEY_CLIENT)).thenReturn(attribute);
        when(attribute.get()).thenReturn(gate);

        applierRegisterCommandHandler.handle(dumpCommandPacket, nettyClient);
        Thread.sleep(250);

        verify(channel, Mockito.times( 2/*ddl with 2 events, drc_ddl and drc_talbe_map*/ + 1 /*previous_gtid_log_event in the mid file*/ + 1 /*drc_uuid_log_event in the mid file*/ + 4 /*exclude transaction*/ + 1 /*empty msg to close file channel*/)).writeAndFlush(any(DefaultFileRegion.class));
    }

    @Test
    public void test_09_DrcIndexLogEventWithRealGtidsetWithGapAfterDdl() throws Exception {
        when(dumpCommandPacket.getApplierName()).thenReturn(APPLIER_NAME);
        when(nettyClient.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(socketAddress);
        when(channel.closeFuture()).thenReturn(channelFuture);
        when(dumpCommandPacket.getReplicatroBackup()).thenReturn(InstanceStatus.INACTIVE.getStatus());
        int maxGtidId = testIndexFileWithDiffGtidSetAndDdl();
        int ddlId = (maxGtidId - 1) / 2;
        GtidSet gtidSet = new GtidSet(UUID_STRING + ":1-" + (ddlId + 1) + ":" + (ddlId + 3) + "-" + maxGtidId);
        when(dumpCommandPacket.getGtidSet()).thenReturn(gtidSet);
        when(channel.attr(ReplicatorMasterHandler.KEY_CLIENT)).thenReturn(attribute);
        when(attribute.get()).thenReturn(gate);

        applierRegisterCommandHandler.handle(dumpCommandPacket, nettyClient);
        Thread.sleep(250);

        verify(channel, Mockito.times( 2/*ddl with 2 events, drc_ddl and drc_talbe_map*/ + 1 /*previous_gtid_log_event in the mid file*/ + 1 /*drc_uuid_log_event in the mid file*/ + 4 /*exclude transaction*/ + 1 /*empty msg to close file channel*/)).writeAndFlush(any(DefaultFileRegion.class));
    }

    @Test
    public void test_10_DrcGtidLogEvent() throws Exception {
        when(dumpCommandPacket.getApplierName()).thenReturn(APPLIER_NAME);
        when(nettyClient.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(socketAddress);
        when(channel.closeFuture()).thenReturn(channelFuture);
        when(dumpCommandPacket.getReplicatroBackup()).thenReturn(InstanceStatus.INACTIVE.getStatus());
        int maxGtidId = testDrcGtidLogEvent();
        int ddlId = (maxGtidId - 1) / 2;
        GtidSet gtidSet = new GtidSet(UUID_STRING + ":1-" + (ddlId + 1));
        when(dumpCommandPacket.getGtidSet()).thenReturn(gtidSet);
        when(channel.attr(ReplicatorMasterHandler.KEY_CLIENT)).thenReturn(attribute);
        when(attribute.get()).thenReturn(gate);

        applierRegisterCommandHandler.handle(dumpCommandPacket, nettyClient);
        Thread.sleep(250);

        int numTransaction = maxGtidId - (ddlId + 1);

        verify(channel, Mockito.times( 1 /*previous_gtid_log_event in the mid file*/ + 1 /*drc_uuid_log_event in the mid file*/ + numTransaction * 4 /*exclude transaction*/ + 1 /*drc_gtid_log_event*/ + 1 /*empty msg to close file channel*/)).writeAndFlush(any(DefaultFileRegion.class));
    }

    @Test
    public void test_11_replicatorSlaveRequestBinlog() throws Exception {
        when(dumpCommandPacket.getApplierName()).thenReturn(APPLIER_NAME);
        when(nettyClient.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(socketAddress);
        when(channel.closeFuture()).thenReturn(channelFuture);
        when(dumpCommandPacket.getReplicatroBackup()).thenReturn(InstanceStatus.INACTIVE.getStatus());
        createMultiUuidsFiles();
        GtidSet gtidSet = new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-1500,c372080a-1804-11ea-8add-98039bbedfad:1-15000");
        when(dumpCommandPacket.getGtidSet()).thenReturn(gtidSet);
        when(channel.attr(ReplicatorMasterHandler.KEY_CLIENT)).thenReturn(attribute);
        when(attribute.get()).thenReturn(gate);

        applierRegisterCommandHandler.handle(dumpCommandPacket, nettyClient);
        Thread.sleep(250);
        verify(channel, Mockito.atLeast(4)).writeAndFlush(any(DefaultFileRegion.class)); //4 files and 4 gtid events
    }

    @Test
    public void test_12_replicatorSlaveRequestBinlog() throws Exception {
        when(dumpCommandPacket.getApplierName()).thenReturn(APPLIER_NAME);
        when(nettyClient.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(socketAddress);
        when(channel.closeFuture()).thenReturn(channelFuture);
        when(dumpCommandPacket.getReplicatroBackup()).thenReturn(InstanceStatus.INACTIVE.getStatus());
        createMultiUuidsFiles();
        GtidSet gtidSet = new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-3:5-1500,c372080a-1804-11ea-8add-98039bbedfad:1-15000");
        when(dumpCommandPacket.getGtidSet()).thenReturn(gtidSet);
        when(channel.attr(ReplicatorMasterHandler.KEY_CLIENT)).thenReturn(attribute);
        when(attribute.get()).thenReturn(gate);

        applierRegisterCommandHandler.handle(dumpCommandPacket, nettyClient);
        Thread.sleep(100);
        verify(channel, Mockito.times(2)).writeAndFlush(any(ByteBuf.class)); //can not find first file and send DrcErrorLogEvent(header and body)
    }

    @Test
    public void test_13_replicatorSlaveRequestBinlog() throws Exception {
        when(dumpCommandPacket.getApplierName()).thenReturn(APPLIER_NAME);
        when(nettyClient.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(socketAddress);
        when(channel.closeFuture()).thenReturn(channelFuture);
        when(dumpCommandPacket.getReplicatroBackup()).thenReturn(InstanceStatus.INACTIVE.getStatus());
        createMultiUuidsFiles();
        GtidSet gtidSet = new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-1830,c372080a-1804-11ea-8add-98039bbedfad:1-15600");
        when(dumpCommandPacket.getGtidSet()).thenReturn(gtidSet);
        when(channel.attr(ReplicatorMasterHandler.KEY_CLIENT)).thenReturn(attribute);
        when(attribute.get()).thenReturn(gate);

        applierRegisterCommandHandler.handle(dumpCommandPacket, nettyClient);
        Thread.sleep(250);
        verify(channel, Mockito.atLeast(3)).writeAndFlush(any(DefaultFileRegion.class)); //second file
    }

    @Test
    public void test_14_includedDb() throws Exception {
        Set<String> includedDbs = Sets.newHashSet();
        String dbName1 = "drc4";
        String dbName2 = "drc3";
        includedDbs.add(dbName1);
        includedDbs.add(dbName2);

        when(dumpCommandPacket.getApplierName()).thenReturn(APPLIER_NAME);
        when(dumpCommandPacket.getIncludedDbs()).thenReturn(includedDbs);
        when(nettyClient.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(socketAddress);
        when(channel.closeFuture()).thenReturn(channelFuture);
        when(dumpCommandPacket.getReplicatroBackup()).thenReturn(InstanceStatus.ACTIVE.getStatus());

        int size = createDiffDbRows(includedDbs);
        GtidSet gtidSet = new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-2500");

        includedDbs.remove(dbName1);

        when(dumpCommandPacket.getGtidSet()).thenReturn(gtidSet);
        when(channel.attr(ReplicatorMasterHandler.KEY_CLIENT)).thenReturn(attribute);
        when(attribute.get()).thenReturn(gate);

        applierRegisterCommandHandler.handle(dumpCommandPacket, nettyClient);
        Thread.sleep(250);
        verify(channel, Mockito.times(size/2 * 4 /* dbName2 */ + size/2 * 2 /* dbName1 */ + 1 /* drc_uuid*/ + 1 /* close fd write empty event*/)).writeAndFlush(any(DefaultFileRegion.class));
    }

    // send 123(tableId) -> db1.table1(tableName), skip 124(tableId) -> db1.table2(tableName)
    @Test
    public void test_15_nameFilter() throws Exception {

        String nameFilter = "db1.table1";

        when(dumpCommandPacket.getApplierName()).thenReturn(APPLIER_NAME);
        when(dumpCommandPacket.getNameFilter()).thenReturn(nameFilter);
        when(nettyClient.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(socketAddress);
        when(channel.closeFuture()).thenReturn(channelFuture);
        when(dumpCommandPacket.getReplicatroBackup()).thenReturn(InstanceStatus.ACTIVE.getStatus());

        createDiffTableRows();
        GtidSet gtidSet = new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-2500");


        when(dumpCommandPacket.getGtidSet()).thenReturn(gtidSet);
        when(channel.attr(ReplicatorMasterHandler.KEY_CLIENT)).thenReturn(attribute);
        when(attribute.get()).thenReturn(gate);

        applierRegisterCommandHandler.handle(dumpCommandPacket, nettyClient);
        Thread.sleep(250);
        verify(channel, Mockito.times(6 /* db1.table1 */ + 1 /* drc_uuid*/ + 1 /* close fd write empty event*/)).writeAndFlush(any(DefaultFileRegion.class));
    }

    public int createDiffDbRows(Set<String> dbNames) throws Exception {
        fileManager = new DefaultFileManager(schemaManager, APPLIER_NAME);
        fileManager.initialize();
        fileManager.start();
        fileManager.setGtidManager(gtidManager);

        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);

        GtidSet gtidSet1 = new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-2000");
        gtidManager.updateExecutedGtids(gtidSet1);
        int eventSize = 0;
        for (String db : dbNames) {
            writeTransaction(db);
            eventSize++;
        }

        GtidSet gtidSet2 = new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-3000");
        gtidManager.updateExecutedGtids(gtidSet2);

        for (String db : dbNames) {
            writeTransaction(db);
            eventSize++;
        }

        GtidSet gtidSet3 = new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-4000");
        gtidManager.updateExecutedGtids(gtidSet3);
        for (String db : dbNames) {
            writeTransaction(db);
            eventSize++;
        }
        fileManager.flush();

        return eventSize;
    }

    public int createDiffTableRows() throws Exception {
        fileManager = new DefaultFileManager(schemaManager, APPLIER_NAME);
        fileManager.initialize();
        fileManager.start();
        fileManager.setGtidManager(gtidManager);

        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);

        GtidSet gtidSet1 = new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-2500");
        gtidManager.updateExecutedGtids(gtidSet1);
        int eventSize = 0;

        writeTransactionWithMultiTableMapLogEvent();
        eventSize++;

        fileManager.flush();

        return eventSize;
    }

    public void createFiles() throws Exception {
        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);
        gtidManager.updateExecutedGtids(new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-1500"));
        ByteBuf byteBuf = getGtidEvent();
        fileManager.append(byteBuf);
        fileManager.rollLog();
        byteBuf.release();

        gtidManager.updateExecutedGtids(new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-1550"));
        byteBuf = getGtidEvent();
        fileManager.append(byteBuf);
        fileManager.rollLog();
        byteBuf.release();

        gtidManager.updateExecutedGtids(EXCLUDED_GTID);
        byteBuf = getGtidEvent();
        fileManager.append(byteBuf);
        fileManager.rollLog();
        byteBuf.release();


        gtidManager.updateExecutedGtids(new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-2020"));
        byteBuf = getGtidEvent();
        fileManager.append(byteBuf);
        fileManager.rollLog();
        byteBuf.release();
    }

    public void createMultiUuidsFiles() throws Exception {
        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);
        gtidManager.updateExecutedGtids(new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-1500,c372080a-1804-11ea-8add-98039bbedfad:1-15000"));
        ByteBuf byteBuf = getGtidEvent();
        fileManager.append(byteBuf);
        fileManager.rollLog();
        byteBuf.release();

        gtidManager.updateExecutedGtids(new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-1550,c372080a-1804-11ea-8add-98039bbedfad:1-15500"));
        byteBuf = getGtidEvent();
        fileManager.append(byteBuf);
        fileManager.rollLog();
        byteBuf.release();

        gtidManager.updateExecutedGtids(new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-2020,c372080a-1804-11ea-8add-98039bbedfad:1-16000"));
        byteBuf = getGtidEvent();
        fileManager.append(byteBuf);
        fileManager.rollLog();
        byteBuf.release();

        gtidManager.updateExecutedGtids(new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-4040,c372080a-1804-11ea-8add-98039bbedfad:1-16500"));
        byteBuf = getGtidEvent();
        fileManager.append(byteBuf);
        fileManager.rollLog();
        byteBuf.release();
    }

    public void createDrcTableMapLogEventFiles() throws Exception {
        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);
        gtidManager.updateExecutedGtids(EXCLUDED_GTID);
        ByteBuf byteBuf = getGtidEvent();
        GtidLogEvent gtidLogEvent = new GtidLogEvent().read(byteBuf);
        String gtid = gtidLogEvent.getGtid();
        gtidManager.addExecutedGtid(gtid);
        fileManager.append(byteBuf);
        TableMapLogEvent tableMapLogEvent = getDrcTableMapLogEvent();
        fileManager.append(tableMapLogEvent.getLogEventHeader().getHeaderBuf());
        fileManager.append(tableMapLogEvent.getPayloadBuf());
        fileManager.append(getXidEvent());
        byteBuf.release();
        gtidLogEvent.release();
        tableMapLogEvent.release();
    }

    public void createDrcDdlLogEventFiles() throws Exception {
        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);
        gtidManager.updateExecutedGtids(EXCLUDED_GTID);
        ByteBuf byteBuf = getGtidEvent();
        GtidLogEvent gtidLogEvent = new GtidLogEvent().read(byteBuf);
        String gtid = gtidLogEvent.getGtid();
        gtidManager.addExecutedGtid(gtid);
        fileManager.append(byteBuf);
        DrcDdlLogEvent drcDdlLogEvent = getDrcDdlLogEvent();
        fileManager.append(drcDdlLogEvent.getLogEventHeader().getHeaderBuf());
        fileManager.append(drcDdlLogEvent.getPayloadBuf());

        TableMapLogEvent tableMapLogEvent = getDrcTableMapLogEvent();
        fileManager.append(tableMapLogEvent.getLogEventHeader().getHeaderBuf());
        fileManager.append(tableMapLogEvent.getPayloadBuf());
        fileManager.append(getXidEvent());

        byteBuf.release();
        gtidLogEvent.release();
        drcDdlLogEvent.release();
        tableMapLogEvent.release();
    }

    public int createDrcIndexLogEventFiles() throws Exception {
        int interval = 1024 * 5;
        System.setProperty(SystemConfig.PREVIOUS_GTID_INTERVAL, String.valueOf(interval));
        fileManager = new DefaultFileManager(schemaManager, APPLIER_NAME);
        fileManager.initialize();
        fileManager.start();
        fileManager.setGtidManager(gtidManager);

        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);

        GtidSet gtidSet1 = new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-2000");
        gtidManager.updateExecutedGtids(gtidSet1);

        int size = writeTransaction();
        int loop = interval / size;
        for (int i = 0; i < loop; ++i) {
            writeTransaction();
        }
        writeTransaction();


        for (int i = 0; i < loop / 2; ++i) {
            writeTransaction();
        }
        GtidSet gtidSet2 = new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-3000");
        gtidManager.updateExecutedGtids(gtidSet2);
        for (int i = 0; i < loop / 2; ++i) {
            writeTransaction();
        }
        writeTransaction();


        for (int i = 0; i < loop / 2; ++i) {
            writeTransaction();
        }
        GtidSet gtidSet3 = new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-4000");
        gtidManager.updateExecutedGtids(gtidSet3);
        for (int i = 0; i < loop / 2; ++i) {
            writeTransaction();
        }
        writeTransaction();

        fileManager.flush();

        return loop / 2 + 1;
    }

    /*
    * previous c372080a-1804-11ea-8add-98039bbedf9c:1-2000
    * ddl (4 events)
    * ddl (loop)
    * ddl (1)
    *
    * ddl (loop / 2)
    * executed_gtid c372080a-1804-11ea-8add-98039bbedf9c:1-3000
    * ddl (loop / 2)
    * ddl (1)
    *
    * ddl (loop / 2)
    * executed_gtid c372080a-1804-11ea-8add-98039bbedf9c:1-4000
    * ddl (loop / 2)
    * ddl (1)
    */
    public int createDrcIndexLogEventFilesWithDdl() throws Exception {
        int interval = 1024 * 5;
        System.setProperty(SystemConfig.PREVIOUS_GTID_INTERVAL, String.valueOf(interval));
        fileManager = new DefaultFileManager(schemaManager, APPLIER_NAME);
        fileManager.initialize();
        fileManager.start();
        fileManager.setGtidManager(gtidManager);

        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);

        GtidSet gtidSet1 = new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-2000");
        gtidManager.updateExecutedGtids(gtidSet1);

        int size = writeDdlTransaction();
        int loop = interval / size;
        for (int i = 0; i < loop; ++i) {
            writeDdlTransaction();
        }
        writeDdlTransaction();


        for (int i = 0; i < loop / 2; ++i) {
            writeDdlTransaction();
        }
        GtidSet gtidSet2 = new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-3000");
        gtidManager.updateExecutedGtids(gtidSet2);
        for (int i = 0; i < loop / 2; ++i) {
            writeDdlTransaction();
        }
        writeDdlTransaction();


        for (int i = 0; i < loop / 2; ++i) {
            writeDdlTransaction();
        }
        GtidSet gtidSet3 = new GtidSet("c372080a-1804-11ea-8add-98039bbedf9c:1-4000");
        gtidManager.updateExecutedGtids(gtidSet3);
        for (int i = 0; i < loop / 2; ++i) {
            writeDdlTransaction();
        }
        writeDdlTransaction();

        fileManager.flush();

        return loop / 2 + 1;
    }

    public int testIndexFileWithDiffGtidSetAndDdl() throws Exception {
        int interval = 1024 * 5;
        System.setProperty(SystemConfig.PREVIOUS_GTID_INTERVAL, String.valueOf(interval));
        fileManager = new DefaultFileManager(schemaManager, APPLIER_NAME);
        fileManager.initialize();
        fileManager.start();
        fileManager.setGtidManager(gtidManager);
        gtidManager.updateExecutedGtids(new GtidSet(""));

        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);
        int size = writeTransactionWithGtid(UUID_STRING + ":" + 1);
        gtidManager.addExecutedGtid(UUID_STRING + ":" + 1);
        int loop = interval / size;
        for (int i = 2; i <= loop; ++i) {
            if (i == loop / 2) {
                writeTransactionWithGtidAndDdl(UUID_STRING + ":" + i);
            } else {
                writeTransactionWithGtid(UUID_STRING + ":" + i);
            }
            gtidManager.addExecutedGtid(UUID_STRING + ":" + i);
        }
        writeTransactionWithGtid(UUID_STRING + ":" + (loop + 1));
        gtidManager.addExecutedGtid(UUID_STRING + ":" + (loop + 1));

        fileManager.flush();

        return loop + 1;
    }

    public int testDrcGtidLogEvent() throws Exception {
        int interval = 1024 * 5;
        System.setProperty(SystemConfig.PREVIOUS_GTID_INTERVAL, String.valueOf(interval));
        fileManager = new DefaultFileManager(schemaManager, APPLIER_NAME);
        fileManager.initialize();
        fileManager.start();
        fileManager.setGtidManager(gtidManager);
        gtidManager.updateExecutedGtids(new GtidSet(""));

        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);
        int size = writeTransactionWithGtidAndTransactionOffset(UUID_STRING + ":" + 1);
        gtidManager.addExecutedGtid(UUID_STRING + ":" + 1);
        int loop = interval / size;
        String drcGtidLogEvent;
        for (int i = 2; i <= loop; ++i) {
            if (i == loop / 2) {
                writeTransactionWithGtidAndTransactionOffset(UUID_STRING + ":" + i);
                drcGtidLogEvent = DRC_UUID_STRING + ":" + i;
                writeDrcGtidLogEvent(drcGtidLogEvent);
                gtidManager.addExecutedGtid(drcGtidLogEvent);
            } else {
                writeTransactionWithGtidAndTransactionOffset(UUID_STRING + ":" + i);
            }
            gtidManager.addExecutedGtid(UUID_STRING + ":" + i);
        }
        writeTransactionWithGtidAndTransactionOffset(UUID_STRING + ":" + (loop + 1));

        gtidManager.setUuids(Sets.newHashSet(UUID_STRING));

        fileManager.flush();

        return loop + 1;
    }

}
