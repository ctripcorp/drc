package com.ctrip.framework.drc.replicator.impl.oubound;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidManager;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.command.packet.applier.ApplierDumpCommandPacket;
import com.ctrip.framework.drc.core.monitor.kpi.OutboundMonitorReport;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.utils.ScheduleCloseGate;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.BinlogScanner;
import com.ctrip.framework.drc.replicator.impl.oubound.channel.ChannelAttributeKey;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.*;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.sender.SenderFilterChainContext;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.ApplierRegisterCommandHandler;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.ApplierRegisterCommandHandlerBefore;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.ReplicatorMasterHandler;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.binlog.DefaultBinlogScannerManager;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.binlog.DefaultBinlogSender;
import com.ctrip.framework.drc.replicator.store.manager.file.FileManager;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.Attribute;
import org.apache.commons.compress.utils.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.*;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.*;
import static com.ctrip.framework.drc.replicator.store.manager.file.DefaultFileManager.LOG_PATH;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BinlogScannerAndSenderTest {
    /*
       needed if change binlog file
     */
    public static final String UUID = "5e54ab78-5854-11ee-9136-fa163e6063c9";
    public static final String BINLOG_PATH = "/rbinlog.0000000047";
    private int firstTransactionId = 197799982;

    /*
       optional
     */
    private static final boolean CHECK_INTEGRATION_TEST_DB = true;
    // if something goes wrong, open this to check detail
    private static final boolean CHECK_HISTORY_DETAIL = false;
    private static final int PARALLEL_NUM = 10;
    protected File logDir = new File(LOG_PATH + "scanner.test");


    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private ApplierRegisterCommandHandler applierRegisterCommandHandler;
    private ApplierRegisterCommandHandlerBefore applierRegisterCommandHandlerBefore;
    private File file;

    private String getRandomGtid() {
        int i = new Random().nextInt(1000) + firstTransactionId;
        return UUID + ":1-" + i;
    }

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        initialize();
    }


    private void initialize() {
        if (!logDir.exists()) {
            boolean created = logDir.mkdirs();
            logger.info("create {} {}", logDir, created);
        }
        file = new File(logDir + BINLOG_PATH + "asd");
        if (!file.exists()) {
            file = new File(Thread.currentThread().getContextClassLoader().getResource("rbinlog/rbinlog.0000000040").getPath());
        }
        applierRegisterCommandHandler = getApplierRegisterCommandHandler();
        applierRegisterCommandHandlerBefore = getApplierRegisterCommandHandler2();
    }


    /**
     * 1.merge 后，从gtid开始同步 (
     *
     * @throws InterruptedException
     */
    @Test
    public void testStart() throws InterruptedException {
        int i = 1;

        if (CHECK_INTEGRATION_TEST_DB) {
            for (; i <= Math.min(PARALLEL_NUM, 4); i++) {
                String randomGtid = getRandomGtid();
                startDumpToTest("drc" + i, randomGtid);
                startDumpBefore("drc" + i, randomGtid);
            }
        }

        for (; i <= PARALLEL_NUM; i++) {
            String randomGtid = getRandomGtid();
            startDumpToTest(getDbName(i), randomGtid);
            startDumpBefore(getDbName(i), randomGtid);
        }
        waitAndCheckResult();
    }


    private void waitAndCheckResult() throws InterruptedException {
        // old
        List<ApplierRegisterCommandHandlerBefore.DumpTask> dumpTasks = applierRegisterCommandHandlerBefore.getList();
        List<OutboundFilterChainFactory.OldLocalSendFilter> sendFilters2 = getOldLocalSendFilters(dumpTasks);
        Assert.assertEquals(PARALLEL_NUM, sendFilters2.size());

        // new
        DefaultBinlogScannerManager scannerManager = (DefaultBinlogScannerManager) applierRegisterCommandHandler.getBinlogScannerManager();
        List<LocalSendFilter> sendFilters = getNewLocalSendFilters(scannerManager);
        Assert.assertEquals(PARALLEL_NUM, sendFilters.size());

        // wait end
        while (!scannerManager.isScannerEmpty() || dumpTasks.stream().anyMatch(ApplierRegisterCommandHandlerBefore.DumpTask::loop)) {
            Thread.sleep(1000);
        }

        // check result
        Map<LogEventType, Set<LogEventType>> fsm = getFiniteStateMachine();

        logger.info("----history:--- ");
        List<String> errors = Lists.newArrayList();
        Map<String, Map<LogEventType, Integer>> newMap = new HashMap<>();
        Map<String, Map<LogEventType, Integer>> oldMap = new HashMap<>();
        test(sendFilters, fsm, errors, newMap);
        test(sendFilters2, fsm, errors, oldMap);
        MapDifference<String, Object> configsDiff = Maps.difference(newMap, oldMap);
        System.out.println("new:    " + newMap);
        System.out.println("before: " + oldMap);
        Assert.assertEquals(String.join("\n", errors), 0, errors.size());
        Assert.assertEquals(configsDiff.toString(), oldMap, newMap);
    }

    private void test(List<? extends LocalHistoryForTest> sendFilters, Map<LogEventType, Set<LogEventType>> fsm, List<String> errors, Map<String, Map<LogEventType, Integer>> allMap) {
        for (LocalHistoryForTest sendFilter : sendFilters) {
            String name = sendFilter.getName();
            logger.info("history: {}", name);
            List<OutboundLogEventContext> history = sendFilter.getHistory(name);
            for (int i = 1; i < history.size(); i++) {
                OutboundLogEventContext pre = history.get(i - 1);
                OutboundLogEventContext now = history.get(i);
                if (CHECK_HISTORY_DETAIL) {
                    logger.info("{} {} {} {}", sendFilter.getName(), now.getGtid(), pre.getEventType(), now.getEventType());
                }
                if (!fsm.get(pre.getEventType()).contains(now.getEventType())) {
                    String message = pre.getEventType() + " " + now.getEventType() + " cannot go to ";
                    errors.add(message);
                }
            }

            Map<LogEventType, Integer> map = allMap.getOrDefault(name, new HashMap<>());
            allMap.put(name, map);
            for (OutboundLogEventContext context : history) {
                LogEventType key = context.getEventType();
                if (key == drc_ddl_log_event || key == drc_table_map_log_event || key == format_description_log_event || key == previous_gtids_log_event) {
                    continue;
                }
                map.put(key, map.getOrDefault(key, 0) + 1);
            }
        }
    }

    private static Map<LogEventType, Set<LogEventType>> getFiniteStateMachine() {
        Map<LogEventType, Set<LogEventType>> fsm = new HashMap<>();
        fsm.put(LogEventType.gtid_log_event, Sets.newHashSet(LogEventType.xid_log_event, LogEventType.table_map_log_event, drc_ddl_log_event));
        fsm.put(LogEventType.xid_log_event, Sets.newHashSet(LogEventType.gtid_log_event, drc_ddl_log_event, previous_gtids_log_event));
        fsm.put(LogEventType.table_map_log_event, Sets.newHashSet(LogEventType.update_rows_event_v2, LogEventType.write_rows_event_v2, LogEventType.delete_rows_event_v2, xid_log_event));
        fsm.put(LogEventType.update_rows_event_v2, Sets.newHashSet(LogEventType.xid_log_event, LogEventType.table_map_log_event, LogEventType.update_rows_event_v2));
        fsm.put(LogEventType.write_rows_event_v2, Sets.newHashSet(LogEventType.xid_log_event, LogEventType.table_map_log_event, LogEventType.write_rows_event_v2));
        fsm.put(LogEventType.delete_rows_event_v2, Sets.newHashSet(LogEventType.xid_log_event, LogEventType.table_map_log_event, LogEventType.delete_rows_event_v2));
        fsm.put(drc_ddl_log_event, Sets.newHashSet(drc_table_map_log_event, gtid_log_event, LogEventType.xid_log_event, drc_ddl_log_event));
        fsm.put(drc_table_map_log_event, Sets.newHashSet(LogEventType.xid_log_event, drc_table_map_log_event, LogEventType.drc_schema_snapshot_log_event, LogEventType.gtid_log_event, drc_ddl_log_event));
        fsm.put(LogEventType.drc_schema_snapshot_log_event, Sets.newHashSet(LogEventType.drc_uuid_log_event));
        fsm.put(LogEventType.drc_uuid_log_event, Sets.newHashSet(drc_ddl_log_event, LogEventType.gtid_log_event));
        fsm.put(LogEventType.format_description_log_event, Sets.newHashSet(LogEventType.previous_gtids_log_event));
        fsm.put(LogEventType.previous_gtids_log_event, Sets.newHashSet(drc_table_map_log_event, gtid_log_event));
        return fsm;
    }

    private List<OutboundFilterChainFactory.OldLocalSendFilter> getOldLocalSendFilters(List<ApplierRegisterCommandHandlerBefore.DumpTask> list) {
        List<OutboundFilterChainFactory.OldLocalSendFilter> sendFilters2;
        sendFilters2 = list.stream().map(e -> (OutboundFilterChainFactory.OldLocalSendFilter) e.getFilterChain().getSuccessor()).collect(Collectors.toList());
        sendFilters2.forEach(e -> Assert.assertEquals(OutboundFilterChainFactory.OldLocalSendFilter.class, e.getClass()));
        return sendFilters2;
    }

    private synchronized static List<LocalSendFilter> getNewLocalSendFilters(DefaultBinlogScannerManager scannerManager) {
        List<LocalSendFilter> sendFilters;
        List<BinlogScanner> scanners = scannerManager.getScanners();
        List<DefaultBinlogSender> senders = new ArrayList<>();
        scanners.forEach(scanner -> scanner.getSenders().stream().map(binlogSender -> (DefaultBinlogSender) binlogSender).forEach(senders::add));
        sendFilters = senders.stream().map(e -> {
            SendFilter successor = (SendFilter) e.getFilterChain().getSuccessor();
            if (successor instanceof LocalSendFilter) {
                return (LocalSendFilter) successor;
            }
            // replace sendFilter
            Filter<OutboundLogEventContext> rest = successor.getSuccessor();
            SenderFilterChainContext context = new SenderFilterChainContext();
            context.setChannel(mockChannel("11.22.33.44", false, e.getApplierName()));
            context.setRegisterKey(e.getApplierName());
            LocalSendFilter local = new LocalSendFilter(context);
            e.getFilterChain().setSuccessor(local);
            local.setSuccessor(rest);
            return local;
        }).collect(Collectors.toList());
        sendFilters.forEach(e -> Assert.assertEquals(LocalSendFilter.class, e.getClass()));
        return sendFilters;
    }

    private static String getNameFilter(String db, String table) {
        return db + "\\." + table + "," + getDelayMonitorRegex(ApplyMode.db_transaction_table.getType(), db);
//        return db + "\\..*";
    }

    private static ApplierDumpCommandPacket getDumpCommandPacket(String dbName, String applierName, String gtidSet) {
        ApplierDumpCommandPacket dumpCommandPacket = new ApplierDumpCommandPacket(applierName, new GtidSet(gtidSet));
        String table = ".*";
        if (dbName.equals("drc1")) {
            table = "json";
            dumpCommandPacket.setProperties("{\"rowsFilters\":[{\"mode\":\"java_regex\",\"tables\":\"drc1\\\\.json\",\"configs\":{\"parameterList\":[{\"columns\":[\"id\"],\"illegalArgument\":false,\"context\":\"(?i)^\\\\d*([01])$\",\"fetchMode\":0,\"userFilterMode\":\"uid\"}],\"drcStrategyId\":0,\"routeStrategyId\":0}}],\"columnsFilters\":[]}");
        }
        String nameFilter = getNameFilter(dbName, table);
        dumpCommandPacket.setConsumeType(ConsumeType.Applier.getCode());
        dumpCommandPacket.setNameFilter(nameFilter);
        return dumpCommandPacket;
    }

    private ApplierRegisterCommandHandler getApplierRegisterCommandHandler() {
        GtidManager gtidManager = mock(GtidManager.class);
        FileManager fileManager = mock(FileManager.class);
        when(gtidManager.getExecutedGtids()).thenReturn(new GtidSet("zyn:1-20"));
        when(gtidManager.getCurrentUuid()).thenReturn("zyn");
        when(gtidManager.getPurgedGtids()).thenReturn(new GtidSet(""));
        when(gtidManager.getFirstLogNotInGtidSet(Mockito.any(), Mockito.anyBoolean())).thenAnswer(e -> {
            replaceToLocalSender();
            return file;
        });
        when(gtidManager.getUuids()).thenReturn(Sets.newHashSet(UUID));

        when(fileManager.getCurrentLogFile()).thenReturn(file);
        when(fileManager.getFirstLogFile()).thenReturn(file);
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        replicatorConfig.setRegistryKey("mha1_dalcluster", "mha1");
        return new ApplierRegisterCommandHandler(gtidManager, fileManager, mock(OutboundMonitorReport.class), replicatorConfig);
    }

    private void replaceToLocalSender() {
        DefaultBinlogScannerManager scannerManager = (DefaultBinlogScannerManager) applierRegisterCommandHandler.getBinlogScannerManager();
        getNewLocalSendFilters(scannerManager);
    }

    private ApplierRegisterCommandHandlerBefore getApplierRegisterCommandHandler2() {
        GtidManager gtidManager = mock(GtidManager.class);
        FileManager fileManager = mock(FileManager.class);
        when(gtidManager.getExecutedGtids()).thenReturn(new GtidSet("zyn:1-20"));
        when(gtidManager.getCurrentUuid()).thenReturn("zyn");
        when(gtidManager.getPurgedGtids()).thenReturn(new GtidSet(""));
        when(gtidManager.getFirstLogNotInGtidSet(Mockito.any(), Mockito.anyBoolean())).thenAnswer(e -> {
            return file;
        });
        when(gtidManager.getUuids()).thenReturn(Sets.newHashSet(UUID));

        when(fileManager.getCurrentLogFile()).thenReturn(file);
        when(fileManager.getFirstLogFile()).thenReturn(file);
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        replicatorConfig.setRegistryKey("mha1_dalcluster", "mha1");
        return new ApplierRegisterCommandHandlerBefore(gtidManager, fileManager, mock(OutboundMonitorReport.class), replicatorConfig);
    }

    protected static ScheduledExecutorService scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("scanner-test");

    private static Channel mockChannel(String hostname, boolean scheduleGate, String applierName) {
        Channel channel;
        channel = mock(Channel.class);
        ChannelFuture channelFuture = mock(ChannelFuture.class);
        when(channel.writeAndFlush(any(ByteBuf.class))).thenReturn(channelFuture);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress(hostname, 8080));
        ScheduleCloseGate gate = new ScheduleCloseGate(hostname);
        ChannelAttributeKey channelAttributeKey = new ChannelAttributeKey(gate);
        Attribute<ChannelAttributeKey> key = mock(Attribute.class);
        when(key.get()).thenReturn(channelAttributeKey);
        when(channel.attr(ReplicatorMasterHandler.KEY_CLIENT)).thenReturn(key);
        when(channel.closeFuture()).thenReturn(channelFuture);

        if (scheduleGate) {
            int closeDelay = new Random().nextInt(5000);
            scheduledExecutorService.schedule(() -> {
                System.out.println("schedule close gate: " + applierName);
                gate.scheduleClose();
                int openDelay = new Random().nextInt(2000);
                scheduledExecutorService.schedule(() -> {
                    System.out.println("open gate: " + applierName);
                    gate.open();
                }, openDelay, TimeUnit.MILLISECONDS);
            }, closeDelay, TimeUnit.MILLISECONDS);

        }
        return channel;
    }

    protected static String getDelayMonitorRegex(int applyMode, String includeDbs) {
        String delayTableName = DRC_DELAY_MONITOR_TABLE_NAME;
        ApplyMode applyModeEnum = ApplyMode.getApplyMode(applyMode);
        if (applyModeEnum == ApplyMode.db_transaction_table || applyModeEnum == ApplyMode.db_mq) {
            delayTableName = DRC_DB_DELAY_MONITOR_TABLE_NAME_PREFIX + includeDbs;
        }
        return DRC_MONITOR_SCHEMA_NAME + "\\." + "(" + delayTableName + ")";
    }

    private void startDumpToTest(String dbName, String gtidSet) {
        String applierName = getApplierName(dbName);
        ApplierDumpCommandPacket dumpCommandPacket = getDumpCommandPacket(dbName, applierName, gtidSet);
        NettyClient nettClient = mock(NettyClient.class);
        Channel value = mockChannel("11.22.33.44", true, applierName);
        when(nettClient.channel()).thenReturn(value);
        applierRegisterCommandHandler.handle(dumpCommandPacket, nettClient);
    }

    private void startDumpBefore(String dbName, String gtidSet) {
        String applierName = getApplierName(dbName);
        ApplierDumpCommandPacket dumpCommandPacket = getDumpCommandPacket(dbName, applierName, gtidSet);
        NettyClient nettClient = mock(NettyClient.class);
        Channel value = mockChannel("11.22.33.44", false, applierName);
        when(nettClient.channel()).thenReturn(value);
        applierRegisterCommandHandlerBefore.handle(dumpCommandPacket, nettClient);
    }

    private static String getApplierName(String dbName) {
        return "mha2.mha1." + dbName;
    }

    private static String getDbName(int i) {
        return "zyn_test_shard_db" + i;
    }

}
