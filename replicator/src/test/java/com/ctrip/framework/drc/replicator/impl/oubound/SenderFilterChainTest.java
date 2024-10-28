package com.ctrip.framework.drc.replicator.impl.oubound;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidManager;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.command.packet.applier.ApplierDumpCommandPacket;
import com.ctrip.framework.drc.core.monitor.kpi.OutboundMonitorReport;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.enums.RowsFilterType;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.utils.ScheduleCloseGate;
import com.ctrip.framework.drc.replicator.impl.oubound.channel.ChannelAttributeKey;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.*;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.scanner.ScannerTableNameFilter;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.ApplierRegisterCommandHandler;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.ReplicatorMasterHandler;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.binlog.DefaultBinlogScanner;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.binlog.DefaultBinlogScannerManager;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.binlog.DefaultBinlogSender;
import com.ctrip.framework.drc.replicator.store.manager.file.BinlogPosition;
import com.ctrip.framework.drc.replicator.store.manager.file.FileManager;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.Attribute;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.net.InetSocketAddress;

import static com.ctrip.framework.drc.core.server.common.filter.row.RuleFactory.ROWS_FILTER_RULE;

/**
 * @Author limingdong
 * @create 2022/4/28
 */
public class SenderFilterChainTest extends AbstractRowsFilterTest {
    @Mock
    GtidManager gtidManager;
    @Mock
    FileManager fileManager;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        super.setUp();
        initialize();
    }

    @Override
    protected RowsFilterType getRowsFilterType() {
        System.setProperty(ROWS_FILTER_RULE, "com.ctrip.framework.drc.replicator.impl.oubound.filter.CustomRowsFilterRule");
        return RowsFilterType.Custom;
    }

    @Mock
    private Channel channel;
    @Mock
    private ChannelFuture channelFuture;

    @Mock
    private Attribute<ChannelAttributeKey> key;


    private DefaultBinlogSender sender;
    private DefaultBinlogScanner scanner;
    private ApplierDumpCommandPacket dumpCommandPacket;
    DefaultBinlogScannerManager manager;

    private void initialize() throws Exception {
        when(channel.writeAndFlush(any(ByteBuf.class))).thenReturn(channelFuture);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("11.23.51.6", 8080));
        ChannelAttributeKey channelAttributeKey = new ChannelAttributeKey(new ScheduleCloseGate(channel.remoteAddress().toString()));
        key = mock(Attribute.class);
        when(key.get()).thenReturn(channelAttributeKey);
        when(channel.attr(ReplicatorMasterHandler.KEY_CLIENT)).thenReturn(key);
        when(channel.closeFuture()).thenReturn(channelFuture);

        ApplierRegisterCommandHandler applierRegisterCommandHandler = mock(ApplierRegisterCommandHandler.class);
        when(applierRegisterCommandHandler.getFileManager()).thenReturn(fileManager);
        when(applierRegisterCommandHandler.getGtidManager()).thenReturn(gtidManager);
        when(applierRegisterCommandHandler.getReplicatorName()).thenReturn("unit-mha1.mha1");
        when(applierRegisterCommandHandler.getGtidManager()).thenReturn(gtidManager);
        when(applierRegisterCommandHandler.getOutboundMonitorReport()).thenReturn(Mockito.mock(OutboundMonitorReport.class));


        when(gtidManager.getExecutedGtids()).thenReturn(new GtidSet("zyn:1-20"));
        when(gtidManager.getCurrentUuid()).thenReturn("zyn");
        when(gtidManager.getPurgedGtids()).thenReturn(new GtidSet(""));
        when(gtidManager.getFirstLogNotInGtidSet(Mockito.any(), Mockito.anyBoolean())).thenReturn(rbinlog);

        when(fileManager.getCurrentLogFile()).thenReturn(rbinlog);
        when(fileManager.getFirstLogFile()).thenReturn(rbinlog);
        manager = new DefaultBinlogScannerManager(applierRegisterCommandHandler);
        dumpCommandPacket = new ApplierDumpCommandPacket("mha1.mha2", new GtidSet("zyn:1-10"));
        dumpCommandPacket.setConsumeType(ConsumeType.Applier.getCode());
        dumpCommandPacket.setNameFilter("drc1\\..*,drc2\\..*,drc3\\..*,drc4\\..*");

    }

    // 1:1000(false,false) -> 1:500(true,true)
    @Test
    public void testSenderMove() throws Exception {
        refreshSenderAndScanner(null);
        System.out.println("sender.getBinlogPosition() = " + sender.getBinlogPosition());


        OutboundLogEventContext scannerContext = new OutboundLogEventContext();
        scannerContext.setInGtidExcludeGroup(false);
        scannerContext.setInSchemaExcludeGroup(false);
        scannerContext.setFileSeq(1);
        scannerContext.setFileChannelPosAfterRead(1000);
        sender.updatePosition(scannerContext.getBinlogPosition());
        sender.refreshInExcludedGroup(scannerContext);
        // mock merge back
        scannerContext.setInGtidExcludeGroup(true);
        scannerContext.setInSchemaExcludeGroup(true);
        scannerContext.setFileSeq(1);
        scannerContext.setFileChannelPosAfterRead(500);
        sender.updatePosition(scannerContext.getBinlogPosition());
        sender.refreshInExcludedGroup(scannerContext);
        // assert
        Assert.assertFalse(sender.getSenderContext().isInGtidExcludeGroup());
        Assert.assertFalse(sender.getSenderContext().isInSchemaExcludeGroup());
        Assert.assertEquals(BinlogPosition.from(1, 1000), sender.getSenderContext().getBinlogPosition());


        scanner.getFilterChain().doFilter(scannerContext);
    }

    @Test
    public void testVerifyScannerFilterChain() throws Exception {
        refreshSenderAndScanner(null);

        Filter<OutboundLogEventContext> filter = scanner.getFilterChain();
        Filter first = filter;
        Assert.assertTrue(filter instanceof ScannerPositionFilter);
        filter = filter.getSuccessor();
        Assert.assertTrue(filter instanceof ReadFilter);
        filter = filter.getSuccessor();
        Assert.assertTrue(filter instanceof IndexFilter);
        filter = filter.getSuccessor();
        Assert.assertTrue(filter instanceof TypeFilter);
        filter = filter.getSuccessor();
        Assert.assertTrue(filter instanceof SchemaFilter);
        filter = filter.getSuccessor();
        Assert.assertTrue(filter instanceof SkipFilter);
        filter = filter.getSuccessor();
        Assert.assertTrue(filter instanceof ScannerTableNameFilter);
        filter = filter.getSuccessor();
        Assert.assertNull(filter);
        first.release();
    }

    private void refreshSenderAndScanner(String properties) throws Exception {
        dumpCommandPacket.setProperties(properties);
        sender = manager.createSenderInner(channel, dumpCommandPacket);
        scanner = manager.createScannerInner(Lists.newArrayList(sender));
        scanner.initialize();
    }


}
