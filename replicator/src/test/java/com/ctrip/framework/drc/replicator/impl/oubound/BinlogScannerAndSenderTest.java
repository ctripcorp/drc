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
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
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
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.Attribute;
import org.junit.Assert;
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
import static com.ctrip.framework.drc.replicator.store.manager.file.DefaultFileManager.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BinlogScannerAndSenderTest {
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private static final int ROUND = 3;

    /*
       needed if change binlog file
       3d08481a-61e8-11eb-a536-043f72ccc6e8:1-52943628,4bfa72e6-644f-11eb-8ccd-78ac44016ff0:1-370200,5154f1cd-65ef-11ef-9fdf-b8cef68a2272:1-26468,d4b1010a-3d10-11ec-904e-b8cef6e13d0c:1-390442,dd8764a4-61e8-11eb-988e-043f72ccc708:1-11418273298
     */
    public static final String UUID = "dd04bb77-27e3-11ef-894f-fa163edb40c1";
    private int firstTransactionId = 1010750124;
    public static final String BINLOG_PATH = "/rbinlog.0000000001";
    public static final boolean CHECK_NOT_EXIST_DB = true;
    public static final boolean CHECK_REPLICATOR_SLAVE = true;
    private static final boolean CHECK_DRC_SHARD_TEST_DB = true;
    private static final boolean CHECK_INTEGRATION_TEST_DB = true;
    private static final boolean CHECK_INTEGRATION_DDL_TEST_DB = true;

    private static final boolean SCHEDULE_CLOSE_GATE = true;

    private final List<ConsumeType> types = Lists.newArrayList(ConsumeType.Applier, ConsumeType.Messenger);

    // if something goes wrong, open this to check detail
    private static final boolean CHECK_HISTORY_DETAIL = true;
    private static final int PARALLEL_NUM = 10;
    protected File logDir = new File(LOG_PATH + "scanner.test");


    private ApplierRegisterCommandHandler applierRegisterCommandHandler;
    private ApplierRegisterCommandHandlerBefore applierRegisterCommandHandlerBefore;
    private File file;
    @Test
    public void test() throws Exception {
        int round = ROUND;
        for (int i = 1; i <= round; i++) {
            long before = System.currentTimeMillis();
            System.out.printf("-------test start [%d/%d] -------%n", i, round);
            testStart();
            long cost = System.currentTimeMillis() - before;
            System.out.printf("-------test success [%d/%d] cost = %d ms-------%n", i, round, cost);
        }
    }

    private String getRandomGtid() {
        int i = new Random().nextInt(20000) + firstTransactionId;
        return UUID + ":1-" + i;
    }

    private String getFullGtid() {
        int i = firstTransactionId;
        return UUID + ":1-" + i;
    }


    private void initialize() {
        if (!logDir.exists()) {
            boolean created = logDir.mkdirs();
            logger.info("create {} {}", logDir, created);
        }
        setFile();
        applierRegisterCommandHandler = getApplierRegisterCommandHandler();
        applierRegisterCommandHandlerBefore = getApplierRegisterCommandHandler2();

        openTimeMill = System.currentTimeMillis() + 1000;
    }

    private void setFile() {
        file = new File(logDir + BINLOG_PATH);
        if (!file.exists()) {
            file = new File(Thread.currentThread().getContextClassLoader().getResource("rbinlog/rbinlog.0000000010").getPath());
        }
    }


    /**
     * 1.merge 后，从gtid开始同步 (
     *
     * @throws InterruptedException
     */
    public void testStart() throws Exception {
        MockitoAnnotations.openMocks(this);
        initialize();

        int i = 1;
        if (CHECK_REPLICATOR_SLAVE) {
            String randomGtid = getRandomGtid();
            startSlaveDump(randomGtid);
            startSlaveDumpBefore(randomGtid);
        }

        if (CHECK_NOT_EXIST_DB) {
            String randomGtid = getRandomGtid();
            String dbName = "some_xxx_db_not_exist";
            startDumpToTest(0, dbName, randomGtid);
            startDumpBefore(0, dbName, randomGtid);
        }

        if (CHECK_INTEGRATION_TEST_DB) {
            for (; i <= Math.min(PARALLEL_NUM, 4); i++) {
                String dbName = "drc" + i;
                for (int j = 0; j < 2; j++) {
                    String randomGtid = getRandomGtid();
                    startDumpToTest(j, dbName, randomGtid);
                    startDumpBefore(j, dbName, randomGtid);
                }
            }
        }
        if (CHECK_INTEGRATION_DDL_TEST_DB) {
            List<String> dbNames = Lists.newArrayList("ghost1_unitest", "generic_ddl");
            for (int t = 0; t < 2; t++) {
                String dbName = dbNames.get(t);
                for (int j = 0; j < 2; j++) {
                    String randomGtid = getRandomGtid();
                    startDumpToTest(j, dbName, randomGtid);
                    startDumpBefore(j, dbName, randomGtid);
                }
            }
        }
        if (CHECK_DRC_SHARD_TEST_DB) {
            int startI = i;
            for (; i <= PARALLEL_NUM; i++) {
                int t = i - startI + 1;
                String dbName = getDbName(t);
                String randomGtid = getRandomGtid();
                if (t == 5) {
                    randomGtid = getFullGtid();
                }
                startDumpToTest(0, dbName, randomGtid);
                startDumpBefore(0, dbName, randomGtid);

            }
        }

        waitAndCheckResult();
    }


    private void waitAndCheckResult() throws Exception {
        int parallelNum = 0;
        if (CHECK_INTEGRATION_TEST_DB) {
            parallelNum += types.size() * Math.min(PARALLEL_NUM, 4) * 2;
        }
        if (CHECK_DRC_SHARD_TEST_DB) {
            parallelNum += types.size() * Math.max(0, PARALLEL_NUM - 4);
        }
        if (CHECK_INTEGRATION_DDL_TEST_DB) {
            parallelNum += types.size() * 2 * 2;
        }
        if (CHECK_NOT_EXIST_DB) {
            parallelNum += types.size();
        }
        if (CHECK_REPLICATOR_SLAVE) {
            parallelNum += 1;
        }
        // old
        List<ApplierRegisterCommandHandlerBefore.DumpTask> dumpTasks = applierRegisterCommandHandlerBefore.getList();
        List<OutboundFilterChainFactory.OldLocalSendFilter> oldSendFilters = getOldLocalSendFilters(dumpTasks);
        Assert.assertEquals(parallelNum, oldSendFilters.size());

        Set<String> requiredApplierNames = dumpTasks.stream().map(ApplierRegisterCommandHandlerBefore.DumpTask::getApplierName).collect(Collectors.toSet());
        // new
        DefaultBinlogScannerManager scannerManager = (DefaultBinlogScannerManager) applierRegisterCommandHandler.getBinlogScannerManager();
        List<LocalSendFilter> newSendFilters = getNewLocalSendFilters(scannerManager);
        Set<String> exitApplierNames = newSendFilters.stream().map(LocalSendFilter::getName).collect(Collectors.toSet());
        requiredApplierNames.removeAll(exitApplierNames);
        Assert.assertEquals(requiredApplierNames.toString(), parallelNum, newSendFilters.size());

        // wait end
        while (!scannerManager.isScannerEmpty() || dumpTasks.stream().anyMatch(ApplierRegisterCommandHandlerBefore.DumpTask::loop)) {
            Thread.sleep(1000);
        }

        // check result
        Map<ConsumeType, Map<LogEventType, Set<LogEventType>>> fsm = getFiniteStateMachine();

        logger.info("----history:--- ");
        List<String> errors = Lists.newArrayList();
        Map<String, Map<LogEventType, Integer>> newMap = new TreeMap<>();
        Map<String, Map<LogEventType, Integer>> oldMap = new TreeMap<>();
        test(newSendFilters, fsm, errors, newMap);
        test(oldSendFilters, fsm, errors, oldMap);
        newMap = filterOutEventType(newMap, Sets.newHashSet(gtid_log_event, xid_log_event));
        oldMap = filterOutEventType(oldMap, Sets.newHashSet(gtid_log_event, xid_log_event));
        System.out.println("new:    " + JsonUtils.toJson(newMap));
        System.out.println("before: " + JsonUtils.toJson(oldMap));
        MapDifference<String, Object> configsDiff = getDifference(newMap, oldMap);
        Assert.assertEquals(String.join("\n", errors), 0, errors.size());
        Assert.assertEquals(configsDiff.toString(), oldMap, newMap);

        // release
        applierRegisterCommandHandler.dispose();
        applierRegisterCommandHandlerBefore.dispose();
        newSendFilters.forEach(LocalSendFilter::clearHistory);
        oldSendFilters.forEach(LocalHistoryForTest::clearHistory);
        newMap.clear();
        oldMap.clear();
    }

    private static MapDifference<String, Object> getDifference(Map<String, Map<LogEventType, Integer>> newMap, Map<String, Map<LogEventType, Integer>> oldMap) {
        return Maps.difference(newMap, oldMap);
    }

    private static Map<String, Map<LogEventType, Integer>> filterOutEventType(Map<String, Map<LogEventType, Integer>> newMap, Set<LogEventType> ignoreType) {
        Map<String, Map<LogEventType, Integer>> result = new HashMap<>();
        for (Map.Entry<String, Map<LogEventType, Integer>> entry : newMap.entrySet()) {
            Map<LogEventType, Integer> map = new HashMap<>();
            for (Map.Entry<LogEventType, Integer> e : entry.getValue().entrySet()) {
                if (!ignoreType.contains(e.getKey())) {
                    map.put(e.getKey(), e.getValue());
                }
            }
            result.put(entry.getKey(), map);
        }
        return result;
    }

    private void test(List<? extends LocalHistoryForTest> sendFilters, Map<ConsumeType, Map<LogEventType, Set<LogEventType>>> fsmByConsumeType, List<String> errors, Map<String, Map<LogEventType, Integer>> allMap) {
        for (LocalHistoryForTest sendFilter : sendFilters) {
            String name = sendFilter.getName();
            logger.info("history: {}", name);
            List<OutboundLogEventContext> history = sendFilter.getHistory(name);
            Map<LogEventType, Set<LogEventType>> fsm = fsmByConsumeType.get(sendFilter.getConsumeType());
            if (fsm == null) {
                throw new IllegalStateException("consume type null: " + sendFilter.getConsumeType());
            }
            for (int i = 1; i < history.size(); i++) {
                OutboundLogEventContext pre = history.get(i - 1);
                OutboundLogEventContext now = history.get(i);
                LogEventType eventType = pre.getEventType();
                LogEventType nowType = now.getEventType();
                if (!fsm.getOrDefault(eventType, Collections.emptySet()).contains(nowType)) {
                    String message = name + ": " + eventType + " " + nowType + " cannot go to ";
                    errors.add(message);
                    logger.info("{} {} {} {} [{}] -> [{}]", sendFilter.getName(), now.getGtid(), eventType, nowType, pre.getBinlogPosition(), now.getBinlogPosition());
                }
            }

            Map<LogEventType, Integer> map = allMap.getOrDefault(name, new HashMap<>());
            allMap.put(name, map);
            for (OutboundLogEventContext context : history) {
                LogEventType key = context.getEventType();
                if (ignoreType.contains(key)) {
                    continue;
                }
                map.put(key, map.getOrDefault(key, 0) + 1);
            }
        }
    }

    private static final Set<LogEventType> ignoreType = Sets.newHashSet(drc_ddl_log_event, drc_uuid_log_event);

    private static Map<ConsumeType, Map<LogEventType, Set<LogEventType>>> getFiniteStateMachine() {
        Map<ConsumeType, Map<LogEventType, Set<LogEventType>>> fsm = new HashMap<>();
        fsm.put(ConsumeType.Applier, getApplierFSM());
        fsm.put(ConsumeType.Messenger, getMesengerFMS());
        fsm.put(ConsumeType.Replicator, getReplicatorFMS());
        return fsm;
    }

    private static Map<LogEventType, Set<LogEventType>> getApplierFSM() {
        Map<LogEventType, Set<LogEventType>> fsm = new HashMap<>();
        fsm.put(gtid_log_event, Sets.newHashSet(xid_log_event, table_map_log_event, drc_ddl_log_event));
        fsm.put(xid_log_event, Sets.newHashSet(gtid_log_event, drc_ddl_log_event, previous_gtids_log_event, format_description_log_event));
        fsm.put(table_map_log_event, Sets.newHashSet(update_rows_event_v2, write_rows_event_v2, delete_rows_event_v2, xid_log_event));
        fsm.put(update_rows_event_v2, Sets.newHashSet(xid_log_event, table_map_log_event, update_rows_event_v2));
        fsm.put(write_rows_event_v2, Sets.newHashSet(xid_log_event, table_map_log_event, write_rows_event_v2));
        fsm.put(delete_rows_event_v2, Sets.newHashSet(xid_log_event, table_map_log_event, delete_rows_event_v2));
        fsm.put(drc_ddl_log_event, Sets.newHashSet(drc_table_map_log_event, gtid_log_event, xid_log_event, drc_ddl_log_event, format_description_log_event));
        fsm.put(drc_table_map_log_event, Sets.newHashSet(format_description_log_event, xid_log_event, drc_table_map_log_event, drc_schema_snapshot_log_event, drc_uuid_log_event, gtid_log_event, drc_ddl_log_event));
        fsm.put(drc_schema_snapshot_log_event, Sets.newHashSet(drc_uuid_log_event));
        fsm.put(drc_uuid_log_event, Sets.newHashSet(drc_ddl_log_event, gtid_log_event, format_description_log_event, drc_uuid_log_event));
        fsm.put(format_description_log_event, Sets.newHashSet(previous_gtids_log_event));
        fsm.put(previous_gtids_log_event, Sets.newHashSet(drc_table_map_log_event, gtid_log_event, drc_uuid_log_event));
        return fsm;
    }

    private static Map<LogEventType, Set<LogEventType>> getReplicatorFMS() {
        Map<LogEventType, Set<LogEventType>> fsm = getApplierFSM();
        fsm.computeIfAbsent(query_log_event, k -> Sets.newHashSet()).addAll(Sets.newHashSet(xid_log_event, table_map_log_event));
        fsm.computeIfAbsent(xid_log_event, k -> Sets.newHashSet()).add(drc_filter_log_event);
        fsm.computeIfAbsent(drc_filter_log_event, k -> Sets.newHashSet()).add(gtid_log_event);
        fsm.computeIfAbsent(drc_filter_log_event, k -> Sets.newHashSet()).add(drc_filter_log_event);
        fsm.computeIfAbsent(drc_filter_log_event, k -> Sets.newHashSet()).add(drc_ddl_log_event);
        fsm.computeIfAbsent(drc_filter_log_event, k -> Sets.newHashSet()).add(drc_gtid_log_event);
        fsm.computeIfAbsent(drc_filter_log_event, k -> Sets.newHashSet()).add(format_description_log_event);
        fsm.computeIfAbsent(drc_filter_log_event, k -> Sets.newHashSet()).add(write_rows_event_v2);
        fsm.computeIfAbsent(drc_filter_log_event, k -> Sets.newHashSet()).add(delete_rows_event_v2);
        fsm.computeIfAbsent(drc_filter_log_event, k -> Sets.newHashSet()).add(update_rows_event_v2);
        fsm.computeIfAbsent(drc_filter_log_event, k -> Sets.newHashSet()).add(drc_uuid_log_event);
        fsm.computeIfAbsent(drc_filter_log_event, k -> Sets.newHashSet()).add(rows_query_log_event);
        fsm.computeIfAbsent(drc_filter_log_event, k -> Sets.newHashSet()).add(table_map_log_event);
        fsm.computeIfAbsent(gtid_log_event, k -> Sets.newHashSet()).add(query_log_event);
        fsm.computeIfAbsent(drc_table_map_log_event, k -> Sets.newHashSet()).add(query_log_event);
        fsm.computeIfAbsent(drc_table_map_log_event, k -> Sets.newHashSet()).add(drc_filter_log_event);
        fsm.computeIfAbsent(drc_ddl_log_event, k -> Sets.newHashSet()).add(query_log_event);
        fsm.computeIfAbsent(drc_ddl_log_event, k -> Sets.newHashSet()).add(drc_filter_log_event);
        fsm.computeIfAbsent(drc_uuid_log_event, k -> Sets.newHashSet()).add(drc_filter_log_event);
        fsm.computeIfAbsent(previous_gtids_log_event, k -> Sets.newHashSet()).add(drc_uuid_log_event);
        fsm.computeIfAbsent(drc_gtid_log_event, k -> Sets.newHashSet()).add(query_log_event);
        fsm.computeIfAbsent(query_log_event, k -> Sets.newHashSet()).add(rows_query_log_event);
        fsm.computeIfAbsent(rows_query_log_event, k -> Sets.newHashSet()).add(table_map_log_event);
        fsm.computeIfAbsent(rows_query_log_event, k -> Sets.newHashSet()).add(xid_log_event);
        fsm.computeIfAbsent(table_map_log_event, k -> Sets.newHashSet()).add(drc_filter_log_event);
        fsm.computeIfAbsent(rows_query_log_event, k -> Sets.newHashSet()).add(drc_filter_log_event);

        fsm.computeIfAbsent(update_rows_event_v2, k -> Sets.newHashSet()).add(rows_query_log_event);
        fsm.computeIfAbsent(update_rows_event_v2, k -> Sets.newHashSet()).add(drc_filter_log_event);
        fsm.computeIfAbsent(update_rows_event_v2, k -> Sets.newHashSet()).add(query_log_event);
        fsm.computeIfAbsent(write_rows_event_v2, k -> Sets.newHashSet()).add(rows_query_log_event);
        fsm.computeIfAbsent(write_rows_event_v2, k -> Sets.newHashSet()).add(drc_filter_log_event);
        fsm.computeIfAbsent(write_rows_event_v2, k -> Sets.newHashSet()).add(query_log_event);
        fsm.computeIfAbsent(delete_rows_event_v2, k -> Sets.newHashSet()).add(rows_query_log_event);
        fsm.computeIfAbsent(delete_rows_event_v2, k -> Sets.newHashSet()).add(drc_filter_log_event);
        fsm.computeIfAbsent(delete_rows_event_v2, k -> Sets.newHashSet()).add(query_log_event);
        return fsm;
    }

    private static Map<LogEventType, Set<LogEventType>> getMesengerFMS() {
        Map<LogEventType, Set<LogEventType>> fsm = getApplierFSM();
        fsm.computeIfAbsent(xid_log_event, k -> Sets.newHashSet()).add(drc_gtid_log_event);
        fsm.computeIfAbsent(drc_table_map_log_event, k -> Sets.newHashSet()).add(drc_gtid_log_event);
        fsm.computeIfAbsent(drc_gtid_log_event, k -> Sets.newHashSet()).add(xid_log_event);
        fsm.computeIfAbsent(drc_gtid_log_event, k -> Sets.newHashSet()).add(table_map_log_event);
        fsm.computeIfAbsent(drc_uuid_log_event, k -> Sets.newHashSet()).add(drc_gtid_log_event);

        fsm.computeIfAbsent(drc_ddl_log_event, k -> Sets.newHashSet()).add(drc_gtid_log_event);
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
            context.setChannel(mockChannel("11.22.33.44", SCHEDULE_CLOSE_GATE, e.getApplierName()));
            context.setRegisterKey(e.getApplierName());
            context.setConsumeType(e.getConsumeType());
            context.setBinlogSender(e);
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

    private static ApplierDumpCommandPacket getApplierDumpCommandPacket(String dbName, String applierName, String gtidSet) {
        ApplierDumpCommandPacket dumpCommandPacket = new ApplierDumpCommandPacket(applierName, new GtidSet(gtidSet));
        String table = ".*";
        if (dbName.equals("drc1")) {
            table = ".*";
            dumpCommandPacket.setProperties("{\"rowsFilters\":[{\"mode\":\"java_regex\",\"tables\":\"drc1\\\\.json\",\"configs\":{\"parameterList\":[{\"columns\":[\"id\"],\"illegalArgument\":false,\"context\":\"(?i)^\\\\d*([01])$\",\"fetchMode\":0,\"userFilterMode\":\"uid\"}],\"drcStrategyId\":0,\"routeStrategyId\":0}}],\"columnsFilters\":[]}");
        } else if (dbName.equals("drc_shard_1")) {
            table = ".*";
            dumpCommandPacket.setProperties("{\"rowsFilters\":[{\"mode\":\"java_regex\",\"tables\":\"drcmonitordb\\\\.dly_drc_shard_3\",\"configs\":{\"parameterList\":[{\"columns\":[\"delay_info\"],\"illegalArgument\":false,\"context\":\"(?i)^\\\\d*([01])$\",\"fetchMode\":0,\"userFilterMode\":\"uid\"}],\"drcStrategyId\":0,\"routeStrategyId\":0}}],\"columnsFilters\":[]}");
        }
        String nameFilter = getNameFilter(dbName, table);
        dumpCommandPacket.setConsumeType(ConsumeType.Applier.getCode());
        dumpCommandPacket.setNameFilter(nameFilter);
        return dumpCommandPacket;
    }

    private static ApplierDumpCommandPacket getSlavePacket(String applierName, String gtidSet) {
        ApplierDumpCommandPacket dumpCommandPacket = new ApplierDumpCommandPacket(applierName, new GtidSet(gtidSet));
        String table = ".*";
        dumpCommandPacket.setConsumeType(ConsumeType.Replicator.getCode());
        dumpCommandPacket.setRegion("default");
        return dumpCommandPacket;
    }

    private static ApplierDumpCommandPacket getMessengerDumpCommandPacket(String dbName, String applierName, String gtidSet) {
        ApplierDumpCommandPacket dumpCommandPacket = new ApplierDumpCommandPacket(applierName, new GtidSet(gtidSet));
        String table = ".*";
        if (dbName.equals("drc1")) {
            table = "json";
        }
        String nameFilter = getNameFilter(dbName, table);
        dumpCommandPacket.setConsumeType(ConsumeType.Messenger.getCode());
        dumpCommandPacket.setProperties(getMessengerProperty(dbName, table));
        dumpCommandPacket.setNameFilter(nameFilter);
        return dumpCommandPacket;
    }


    private static String getMessengerProperty(String dbName, String tableName) {
        return "{\"nameFilter\":\"drc_shard_1\\\\..*\",\"mqConfigs\":[{\"table\":\"{dbName}\\\\.{tableName}\",\"topic\":\"bbz.drctest.binlog\",\"processor\":null,\"mqType\":\"qmq\",\"serialization\":\"json\",\"persistent\":false,\"persistentDb\":null,\"order\":false,\"orderKey\":null,\"delayTime\":0}]}"
                .replace("{dbName}", dbName)
                .replace("{tableName}", tableName);
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
        when(fileManager.getNextLogFile(any())).thenAnswer(e -> {
            File argument = e.getArgument(0);
            return getNextFile(argument);
        });
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        replicatorConfig.setRegistryKey("mha1_dalcluster", "mha1");
        return new ApplierRegisterCommandHandler(gtidManager, fileManager, mock(OutboundMonitorReport.class), replicatorConfig);
    }

    private File getNextFile(File file) {
        long fileNum = getFileNum(file);
        fileNum++;
        String fileName = String.format(LOG_FILE_FORMAT, LOG_FILE_PREFIX, fileNum);
        File nextFile = new File(file.getParent(), fileName);
        return nextFile;
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
        when(fileManager.getNextLogFile(any())).thenAnswer(e -> {
            File argument = e.getArgument(0);
            return getNextFile(argument);
        });
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


        scheduleOpen(applierName, gate);
        if (scheduleGate) {
            scheduleClose(applierName, gate);
        }
        return channel;
    }

    private static void scheduleClose(String applierName, ScheduleCloseGate gate) {
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

    private static long openTimeMill = 0L;

    private static void scheduleOpen(String applierName, ScheduleCloseGate gate) {
        long openDelay = openTimeMill - System.currentTimeMillis();
        if (openDelay < 0) {
            throw new IllegalStateException("open delay < 0");
        }
        gate.close();
        scheduledExecutorService.schedule(() -> {
            System.out.println("schedule close gate: " + applierName);
            gate.open();
        }, openDelay, TimeUnit.MILLISECONDS);
    }

    protected static String getDelayMonitorRegex(int applyMode, String includeDbs) {
        String delayTableName = DRC_DELAY_MONITOR_TABLE_NAME;
        ApplyMode applyModeEnum = ApplyMode.getApplyMode(applyMode);
        if (applyModeEnum == ApplyMode.db_transaction_table || applyModeEnum == ApplyMode.db_mq) {
            delayTableName = DRC_DB_DELAY_MONITOR_TABLE_NAME_PREFIX + includeDbs;
        }
        return DRC_MONITOR_SCHEMA_NAME + "\\." + "(" + delayTableName + ")";
    }


    private void startDumpToTest(int index, String dbName, String gtidSet) {
        for (ConsumeType consumeType : types) {
            String applierName = getApplierName(dbName, consumeType) + "_" + index;
            ApplierDumpCommandPacket dumpCommandPacket = getDumpCommandPacket(dbName, applierName, gtidSet, consumeType);
            NettyClient nettClient = mock(NettyClient.class);
            Channel value = mockChannel("11.22.33.44", SCHEDULE_CLOSE_GATE, applierName);
            when(nettClient.channel()).thenReturn(value);
            applierRegisterCommandHandler.handle(dumpCommandPacket, nettClient);
        }
    }

    private ApplierDumpCommandPacket getDumpCommandPacket(String dbName, String applierName, String gtidSet, ConsumeType consumeType) {
        if (consumeType == ConsumeType.Applier) {
            return getApplierDumpCommandPacket(dbName, applierName, gtidSet);
        } else {
            return getMessengerDumpCommandPacket(dbName, applierName, gtidSet);
        }
    }

    private void startDumpBefore(int index, String dbName, String gtidSet) {
        for (ConsumeType consumeType : types) {
            String applierName = getApplierName(dbName, consumeType) + "_" + index;
            ApplierDumpCommandPacket dumpCommandPacket = getDumpCommandPacket(dbName, applierName, gtidSet, consumeType);
            NettyClient nettClient = mock(NettyClient.class);
            Channel value = mockChannel("11.22.33.44", SCHEDULE_CLOSE_GATE, applierName);
            when(nettClient.channel()).thenReturn(value);
            applierRegisterCommandHandlerBefore.handle(dumpCommandPacket, nettClient);
        }
    }

    private void startSlaveDump(String gtidSet) {
        ApplierDumpCommandPacket dumpCommandPacket = getSlavePacket("salve", gtidSet);
        NettyClient nettClient = mock(NettyClient.class);
        Channel value = mockChannel("11.22.33.44", SCHEDULE_CLOSE_GATE, "salve");
        when(nettClient.channel()).thenReturn(value);
        applierRegisterCommandHandler.handle(dumpCommandPacket, nettClient);
    }

    private void startSlaveDumpBefore(String gtidSet) {
        ApplierDumpCommandPacket dumpCommandPacket = getSlavePacket("salve", gtidSet);
        NettyClient nettClient = mock(NettyClient.class);
        Channel value = mockChannel("11.22.33.44", SCHEDULE_CLOSE_GATE, "salve");
        when(nettClient.channel()).thenReturn(value);
        applierRegisterCommandHandlerBefore.handle(dumpCommandPacket, nettClient);
    }

    private static String getApplierName(String dbName, ConsumeType consumeType) {
        if (consumeType == ConsumeType.Applier) {
            return "mha2.mha1." + dbName;
        } else if (consumeType == ConsumeType.Messenger) {
            return "drc_mq.mha1." + dbName;
        }
        return null;
    }

    private static String getDbName(int i) {
        return "drc_shard_" + i;
    }

}
