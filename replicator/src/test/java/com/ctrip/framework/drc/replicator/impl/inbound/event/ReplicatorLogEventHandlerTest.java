package com.ctrip.framework.drc.replicator.impl.inbound.event;

import com.ctrip.framework.drc.core.driver.binlog.LogEventCallBack;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.*;
import com.ctrip.framework.drc.core.driver.binlog.manager.SchemaManager;
import com.ctrip.framework.drc.core.monitor.kpi.InboundMonitorReport;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.replicator.container.config.TableFilterConfiguration;
import com.ctrip.framework.drc.replicator.container.zookeeper.UuidConfig;
import com.ctrip.framework.drc.replicator.container.zookeeper.UuidOperator;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.EventFilterChainFactory;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.InboundFilterChainContext;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.InboundLogEventContext;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.transaction.TransactionFilterChainFactory;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.EventTransactionCache;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.TransactionCache;
import com.ctrip.framework.drc.replicator.impl.monitor.DefaultMonitorManager;
import com.ctrip.framework.drc.replicator.store.AbstractTransactionTest;
import com.ctrip.framework.drc.replicator.store.FilePersistenceEventStore;
import com.ctrip.framework.drc.replicator.store.manager.file.FileManager;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.table_map_log_event;
import static com.ctrip.framework.drc.core.driver.util.ByteHelper.FORMAT_LOG_EVENT_SIZE;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.EMPTY_DRC_UUID_EVENT_SIZE;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.EMPTY_PREVIOUS_GTID_EVENT_SIZE;
import static com.ctrip.framework.drc.replicator.impl.inbound.filter.PersistPostFilter.FAKE_SERVER_PARAM;
import static com.ctrip.framework.drc.replicator.impl.inbound.filter.PersistPostFilter.FAKE_XID_PARAM;
import static com.ctrip.framework.drc.replicator.store.manager.file.DefaultFileManager.LOG_EVENT_START;

/**
 * Created by @author zhuYongMing on 2019/9/18.
 */
public class ReplicatorLogEventHandlerTest extends AbstractTransactionTest {

    private ReplicatorLogEventHandler logEventHandler;

    private Filter<InboundLogEventContext> flagFilter;

    @Mock
    private SchemaManager schemaManager;

    @Mock
    private DefaultMonitorManager delayMonitor;

    @Mock
    private InboundMonitorReport inboundMonitorReport;

    @Mock
    private ReplicatorConfig replicatorConfig;

    @Mock
    private UuidOperator uuidOperator;

    @Mock
    private UuidConfig uuidConfig;

    private Set<UUID> uuids = Sets.newHashSet();

    private Filter<ITransactionEvent> filterChain = new TransactionFilterChainFactory().createFilterChain(
            new InboundFilterChainContext.Builder().applyMode(ApplyMode.transaction_table.getType()).build());

    private FilePersistenceEventStore filePersistenceEventStore;

    private TransactionCache transactionCache;

    private FileManager fileManager;

    private String clusterName = "unitTest";

    private String registerKey = RegistryKey.from(clusterName, SystemConfig.MHA_NAME_TEST);

    private static final int TABLE_MAP_EVENT_SIZE = 19 + 35 + 1;  //1 for identifier

    private  Set<UUID> uuidSet = Sets.newHashSet();

    private  Set<String> tableNames = Sets.newHashSet();

    private static final String UUID_1 = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe";

    private InboundFilterChainContext filterChainContext;

    private TableFilterConfiguration tableFilterConfiguration = new TableFilterConfiguration();

    @Before
    public void setUp() throws Exception {
        System.setProperty(SystemConfig.REPLICATOR_WHITE_LIST, String.valueOf(true));
        super.initMocks();
        when(replicatorConfig.getWhiteUUID()).thenReturn(uuids);
        when(replicatorConfig.getRegistryKey()).thenReturn(registerKey);
        when(uuidOperator.getUuids(anyString())).thenReturn(uuidConfig);
        when(uuidConfig.getUuids()).thenReturn(Sets.newHashSet());

        uuidSet.add(UUID.fromString(UUID_1));
        filePersistenceEventStore = new FilePersistenceEventStore(schemaManager, uuidOperator, replicatorConfig);
        filePersistenceEventStore.initialize();
        filePersistenceEventStore.start();

        transactionCache = new EventTransactionCache(filePersistenceEventStore, filterChain);
        transactionCache.initialize();
        transactionCache.start();

        fileManager = filePersistenceEventStore.getFileManager();
        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);

        filterChainContext = new InboundFilterChainContext.Builder()
                .whiteUUID(uuidSet)
                .tableNames(tableNames)
                .schemaManager(schemaManager)
                .inboundMonitorReport(inboundMonitorReport)
                .transactionCache(transactionCache)
                .monitorManager(delayMonitor)
                .registryKey(clusterName)
                .tableFilterConfiguration(tableFilterConfiguration)
                .applyMode(ApplyMode.set_gtid.getType()).build();
        flagFilter = new EventFilterChainFactory().createFilterChain(filterChainContext);

        logEventHandler = new ReplicatorLogEventHandler(transactionCache, delayMonitor, flagFilter);
    }

    @After
    public void tearDown() {
        System.setProperty(SystemConfig.REPLICATOR_WHITE_LIST, String.valueOf(false));
        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);
    }

    @Test
    public void testXidWithFilterFalse() {
        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);
        doWriteTransaction(false);

        File file = fileManager.getCurrentLogFile();
        long length = file.length();
        Assert.assertEquals(length, LOG_EVENT_START + EMPTY_PREVIOUS_GTID_EVENT_SIZE + EMPTY_SCHEMA_EVENT_SIZE + EMPTY_DRC_UUID_EVENT_SIZE + DrcIndexLogEvent.FIX_SIZE + FORMAT_LOG_EVENT_SIZE + 3 * ((GTID_ZISE + 4) + XID_ZISE) + FILTER_LOG_EVENT_SIZE * 3);
        logDir = fileManager.getDataDir();
        deleteFiles(logDir);
    }

    @Test
    public void testXidWithFilterTrue() {
        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);

        doWriteTransaction(true);

        File file = fileManager.getCurrentLogFile();
        long length = file.length();
        Assert.assertEquals(length, LOG_EVENT_START + EMPTY_PREVIOUS_GTID_EVENT_SIZE + EMPTY_SCHEMA_EVENT_SIZE + EMPTY_DRC_UUID_EVENT_SIZE + DrcIndexLogEvent.FIX_SIZE + FORMAT_LOG_EVENT_SIZE + 3 * ((GTID_ZISE + 4) + XID_ZISE) + FILTER_LOG_EVENT_SIZE * 3);
        logDir = fileManager.getDataDir();
        deleteFiles(logDir);
    }

    @Test
    public void testXidWithFilterBoth() {
        doTestBoth(false, true);
    }

    @Test
    public void testXidWithFilterTrueAndFalse() {
        doTestBoth(true, false);
    }

    private void doTestBoth(boolean first, boolean second) {
        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);

        doWriteTransaction(first); //3 pair

        doWriteTransaction(second); //3 gtid

        File file = fileManager.getCurrentLogFile();
        long length = file.length();
        Assert.assertEquals(length, LOG_EVENT_START + EMPTY_PREVIOUS_GTID_EVENT_SIZE + EMPTY_SCHEMA_EVENT_SIZE + EMPTY_DRC_UUID_EVENT_SIZE + DrcIndexLogEvent.FIX_SIZE + FORMAT_LOG_EVENT_SIZE + 3 * ((GTID_ZISE + 4) + XID_ZISE) + 3 * ((GTID_ZISE + 4) + XID_ZISE) + 6 * FILTER_LOG_EVENT_SIZE); // 3 * drc_gtid_log_event + 3 * (gtid_log_event + xid) + 6 * filter_log_event
//        Assert.assertEquals(length, LOG_EVENT_START + EMPTY_PREVIOUS_GTID_EVENT_SIZE + EMPTY_SCHEMA_EVENT_SIZE + EMPTY_DRC_UUID_EVENT_SIZE + DrcIndexLogEvent.FIX_SIZE + FORMAT_LOG_EVENT_SIZE + 3 * ((GTID_ZISE + 4) + XID_ZISE) + 3 * (GTID_ZISE + 4)); // 3 * drc_gtid_log_event + 3 * (gtid_log_event + xid)

        logDir = fileManager.getDataDir();
        deleteFiles(logDir);
    }

    @Test
    public void testFilteredDb() throws Exception {
        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);

        writeFilteredTransaction();

        File file = fileManager.getCurrentLogFile();
        long length = file.length();
        int filterLogEventSize = FILTER_LOG_EVENT_SIZE + (19 + 1 + "configdbs".length() + 4 + 1 + "unitest".length() + 4);
        Assert.assertEquals(length, LOG_EVENT_START + EMPTY_PREVIOUS_GTID_EVENT_SIZE + FORMAT_LOG_EVENT_SIZE + EMPTY_SCHEMA_EVENT_SIZE + EMPTY_DRC_UUID_EVENT_SIZE + DrcIndexLogEvent.FIX_SIZE + TABLE_MAP_EVENT_SIZE + 2 * ((GTID_ZISE + 4) + XID_ZISE) + filterLogEventSize);

        logDir = fileManager.getDataDir();
        deleteFiles(logDir);
    }

    // test case that insert a drcheartbeatevent within a transacion, generate wrong transaction binlog
    @Test
    public void testHeartBeatEvent() throws Exception {
        File logDir = fileManager.getDataDir();
        deleteFiles(logDir);

        writeTransactionWithHeartBeat();

        File file = fileManager.getCurrentLogFile();
        long length = file.length();

        XidLogEvent fakeXidLogEvent = new XidLogEvent(FAKE_SERVER_PARAM, FAKE_XID_PARAM, FAKE_XID_PARAM);
        int FAKE_XID_SIZE = (int) fakeXidLogEvent.getLogEventHeader().getEventSize();
        logger.info("[Fake Xid] size is {}, [Xid] size is {}, [Gtid] size is {}", FAKE_XID_SIZE, XID_ZISE, GTID_ZISE);

        Assert.assertEquals(length, LOG_EVENT_START + EMPTY_PREVIOUS_GTID_EVENT_SIZE + FORMAT_LOG_EVENT_SIZE + EMPTY_SCHEMA_EVENT_SIZE + EMPTY_DRC_UUID_EVENT_SIZE + DrcIndexLogEvent.FIX_SIZE + 3 * (GTID_ZISE + 4)/*transaction offset (+4)*/ + XID_ZISE + FAKE_XID_SIZE + FILTER_LOG_EVENT_SIZE * 3);

        logDir = fileManager.getDataDir();
        deleteFiles(logDir);
        fakeXidLogEvent.release();
    }

    @Test
    public void testReset() {
        logEventHandler.reset();
        Assert.assertEquals(logEventHandler.getCurrentGtid(), StringUtils.EMPTY);
    }

    private void writeTransactionWithHeartBeat() throws Exception {
        LogEventCallBack callBack = new LogEventCallBack() {
            @Override
            public Channel getChannel() {
                return null;
            }
            @Override
            public void onHeartHeat() {

            }
        };
        GtidLogEvent gtidLogEvent = getGtidLogEvent();
        gtidLogEvent.setServerUUID(UUID.fromString(UUID_1));
        logEventHandler.onLogEvent(gtidLogEvent, callBack, null);
        DrcHeartbeatLogEvent heartBeatLogEvent = getDrcHeartBeatEvent();
        logEventHandler.onLogEvent(heartBeatLogEvent, callBack, null);

        TableMapLogEvent tableMapLogEvent = getFilteredTableMapLogEvent();
        logEventHandler.onLogEvent(tableMapLogEvent, callBack, null);
        XidLogEvent xidLogEvent = getXidLogEvent();
        logEventHandler.onLogEvent(xidLogEvent, callBack, null);


        gtidLogEvent = getGtidLogEvent();
        gtidLogEvent.setServerUUID(UUID.fromString(UUID_1));
        gtidLogEvent.setEventType(LogEventType.drc_gtid_log_event.getType());
        logEventHandler.onLogEvent(gtidLogEvent, callBack, null);

        gtidLogEvent = getGtidLogEvent();
        gtidLogEvent.setServerUUID(UUID.fromString(UUID_1));
        logEventHandler.onLogEvent(gtidLogEvent, callBack, null);
        xidLogEvent = getXidLogEvent();
        logEventHandler.onLogEvent(xidLogEvent, callBack, null);

        // gtid、 drc_gtid、fake xid、gtid、xid
    }


    private void writeFilteredTransaction() throws Exception {
        GtidLogEvent gtidLogEvent = getGtidLogEvent();
        gtidLogEvent.setServerUUID(UUID.fromString(UUID_1));
        logEventHandler.onLogEvent(gtidLogEvent, null, null);
        TableMapLogEvent tableMapLogEvent = getFilteredTableMapLogEvent();
        logEventHandler.onLogEvent(tableMapLogEvent, null, null);
        XidLogEvent xidLogEvent = getXidLogEvent();
        logEventHandler.onLogEvent(xidLogEvent, null, null);


        gtidLogEvent = getGtidLogEvent();
        gtidLogEvent.setServerUUID(UUID.fromString(UUID_1));
        logEventHandler.onLogEvent(gtidLogEvent, null, null);
        tableMapLogEvent = getNonFilteredTableMapLogEvent();
        logEventHandler.onLogEvent(tableMapLogEvent, null, null);
        xidLogEvent = getXidLogEvent();
        logEventHandler.onLogEvent(xidLogEvent, null, null);
    }

    private void doWriteTransaction(boolean filtered) {
        GtidLogEvent gtidLogEvent = getGtidLogEvent();
        if (!filtered) {
            gtidLogEvent.setServerUUID(UUID.fromString(UUID_1));
        }
        XidLogEvent xidLogEvent = getXidLogEvent();
        logEventHandler.onLogEvent(gtidLogEvent, null, null);
        String currentGtid = logEventHandler.getCurrentGtid();
        Assert.assertTrue(currentGtid.length() > 0);
        logEventHandler.onLogEvent(xidLogEvent, null, null);
        currentGtid = logEventHandler.getCurrentGtid();
        Assert.assertTrue(currentGtid.length() == 0);

        gtidLogEvent = getGtidLogEvent();
        if (!filtered) {
            gtidLogEvent.setServerUUID(UUID.fromString(UUID_1));
        }
        logEventHandler.onLogEvent(gtidLogEvent, null, null);

        gtidLogEvent = getGtidLogEvent();
        if (!filtered) {
            gtidLogEvent.setServerUUID(UUID.fromString(UUID_1));
        }
        logEventHandler.onLogEvent(gtidLogEvent, null, null);
        xidLogEvent = getXidLogEvent();
        logEventHandler.onLogEvent(xidLogEvent, null, null);

        try {
            fileManager.flush();
        } catch (IOException e) {
        }

    }

    private GtidLogEvent getGtidLogEvent() {
        final ByteBuf byteBuf = super.getGtidEvent();
        GtidLogEvent gtidLogEvent = new GtidLogEvent().read(byteBuf);
        byteBuf.release();
        return gtidLogEvent;
    }

    private XidLogEvent getXidLogEvent() {
        final ByteBuf byteBuf = super.getXidEvent();
        XidLogEvent xidLogEvent = new XidLogEvent().read(byteBuf);
        byteBuf.release();
        return xidLogEvent;
    }

    private DrcHeartbeatLogEvent getDrcHeartBeatEvent() {
        return new DrcHeartbeatLogEvent(0);
    }

    private TableMapLogEvent getFilteredTableMapLogEvent() throws IOException {
        List<TableMapLogEvent.Column> columns = mockColumns();
        TableMapLogEvent constructorTableMapLogEvent = new TableMapLogEvent(
                1L, 813, 123, "configdb", "unitest", columns, null, table_map_log_event, 0
        );
        return constructorTableMapLogEvent;
    }

    private TableMapLogEvent getNonFilteredTableMapLogEvent() throws IOException {
        List<TableMapLogEvent.Column> columns = mockColumns();
        TableMapLogEvent constructorTableMapLogEvent = new TableMapLogEvent(
                1L, 813, 123, "configdbs", "unitest", columns, null, table_map_log_event, 0
        );
        return constructorTableMapLogEvent;
    }

    public static List<TableMapLogEvent.Column> mockColumns() {
        List<TableMapLogEvent.Column> columns = Lists.newArrayList();
        TableMapLogEvent.Column column1 = new TableMapLogEvent.Column("id", true, "int", null, "10", null, null, null, null, "int(11)", null, null, "default");
        Assert.assertFalse(column1.isOnUpdate());
        Assert.assertFalse(column1.isPk());
        Assert.assertFalse(column1.isUk());
        Assert.assertEquals("default", column1.getColumnDefault());

        columns.add(column1);

        return columns;
    }
}
