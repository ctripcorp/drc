package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.constant.QueryType;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.binlog.impl.*;
import com.ctrip.framework.drc.core.driver.binlog.manager.ApplyResult;
import com.ctrip.framework.drc.core.driver.binlog.manager.SchemaManager;
import com.ctrip.framework.drc.core.monitor.kpi.InboundMonitorReport;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.replicator.container.config.TableFilterConfiguration;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.TransactionCache;
import com.ctrip.framework.drc.replicator.impl.monitor.DefaultMonitorManager;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Set;
import java.util.UUID;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_TRANSACTION_TABLE_NAME;
import static com.ctrip.framework.drc.replicator.impl.inbound.filter.TransactionFlags.*;

/**
 * @Author limingdong
 * @create 2020/3/12
 */
public class InboundFilterChainFactoryTest extends AbstractFilterTest {

    private static final String UUID_1 = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe";

    private static final String GTID = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1";

    private static final String CLUSTER_NAME = "test-drc";

    private static final String UUID_2 = "12027356-0d03-11ea-a2f0-c6a9fbf1c3fe";

    private static final String GTID_2 = "12027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1";

    private InboundFilterChainContext filterChainContextWithGN;

    private Filter<InboundLogEventContext> flagFilterWithGN;

    private InboundFilterChainContext filterChainContextWithTT;

    private Filter<InboundLogEventContext> flagFilterWithTT;

    @Mock
    private SchemaManager schemaManager;

    @Mock
    private InboundMonitorReport inboundMonitorReport;

    @Mock
    private TransactionCache transactionCache;

    @Mock
    private LogEventHeader logEventHeader;

    @Mock
    private GtidLogEvent gtidLogEvent;

    @Mock
    private QueryLogEvent queryLogEvent;

    @Mock
    private TableMapLogEvent tableMapLogEvent;

    @Mock
    private WriteRowsEvent writeRowsEvent;

    @Mock
    private XidLogEvent xidLogEvent;

    private Set<UUID> uuidSet = Sets.newHashSet();

    private Set<String> tableNames = Sets.newHashSet();

    private InboundLogEventContext logEventWithGroupFlag;

    private DefaultMonitorManager delayMonitor = new DefaultMonitorManager("ut");

    private TableFilterConfiguration tableFilterConfiguration = new TableFilterConfiguration();

    @Before
    public void setUp() throws Exception {
        super.initMocks();
        uuidSet.add(UUID.fromString(UUID_1));

        filterChainContextWithGN = new InboundFilterChainContext.Builder()
                .whiteUUID(uuidSet)
                .tableNames(tableNames)
                .schemaManager(schemaManager)
                .inboundMonitorReport(inboundMonitorReport)
                .transactionCache(transactionCache)
                .monitorManager(delayMonitor)
                .registryKey(CLUSTER_NAME)
                .tableFilterConfiguration(tableFilterConfiguration)
                .applyMode(ApplyMode.set_gtid.getType()).build();
        flagFilterWithGN = new EventFilterChainFactory().createFilterChain(filterChainContextWithGN);

        filterChainContextWithTT = new InboundFilterChainContext.Builder()
                .whiteUUID(uuidSet)
                .tableNames(tableNames)
                .schemaManager(schemaManager)
                .inboundMonitorReport(inboundMonitorReport)
                .transactionCache(transactionCache)
                .monitorManager(delayMonitor)
                .registryKey(CLUSTER_NAME)
                .tableFilterConfiguration(tableFilterConfiguration)
                .applyMode(ApplyMode.transaction_table.getType()).build();
        flagFilterWithTT = new EventFilterChainFactory().createFilterChain(filterChainContextWithTT);

        when(schemaManager.apply(anyString(), anyString(), any(QueryType.class), anyString())).thenReturn(ApplyResult.from(ApplyResult.Status.SUCCESS, ""));
    }

    @Test
    public void testFilterGtidFalse() {
        when(gtidLogEvent.getServerUUID()).thenReturn(UUID.fromString(UUID_1));
        when(gtidLogEvent.getGtid()).thenReturn(GTID);
        when(gtidLogEvent.getLogEventHeader()).thenReturn(logEventHeader);
        when(gtidLogEvent.getLogEventType()).thenReturn(LogEventType.gtid_log_event);
        when(logEventHeader.getEventType()).thenReturn(LogEventType.gtid_log_event.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);

        logEventWithGroupFlag = new InboundLogEventContext(gtidLogEvent, null, new TransactionFlags(), StringUtils.EMPTY);
        boolean skip = flagFilterWithGN.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip);


        when(logEventHeader.getEventType()).thenReturn(LogEventType.query_log_event.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(queryLogEvent.getLogEventType()).thenReturn(LogEventType.query_log_event);
        when(queryLogEvent.getLogEventHeader()).thenReturn(logEventHeader);

        String ALTER_TABLE = "alter /* gh-ost */ table `ghostdb`.`_test1g_gho` ADD COLUMN addcol119 VARCHAR(255) DEFAULT NULL COMMENT '添加普通列测试'";
        when(queryLogEvent.getQuery()).thenReturn(ALTER_TABLE);

        logEventWithGroupFlag.setLogEvent(queryLogEvent);
        skip = flagFilterWithGN.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip);

        when(logEventHeader.getEventType()).thenReturn(LogEventType.table_map_log_event.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(tableMapLogEvent.getLogEventType()).thenReturn(LogEventType.table_map_log_event);
        when(tableMapLogEvent.getLogEventHeader()).thenReturn(logEventHeader);

        logEventWithGroupFlag.setLogEvent(tableMapLogEvent);
        skip = flagFilterWithGN.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip);

        when(logEventHeader.getEventType()).thenReturn(LogEventType.write_rows_event_v2.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(writeRowsEvent.getLogEventType()).thenReturn(LogEventType.write_rows_event_v2);
        when(writeRowsEvent.getLogEventHeader()).thenReturn(logEventHeader);

        logEventWithGroupFlag.setLogEvent(writeRowsEvent);
        skip = flagFilterWithGN.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip);

        when(logEventHeader.getEventType()).thenReturn(LogEventType.xid_log_event.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(xidLogEvent.getLogEventType()).thenReturn(LogEventType.xid_log_event);
        when(xidLogEvent.getLogEventHeader()).thenReturn(logEventHeader);

        logEventWithGroupFlag.setLogEvent(xidLogEvent);
        skip = flagFilterWithGN.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip);
    }

    @Test
    public void testFilterTransactionTable() {
        doTestFilterTransactionTable(DRC_TRANSACTION_TABLE_NAME);
        doTestFilterTransactionTable(DRC_TRANSACTION_TABLE_NAME + 1);
    }

    public void doTestFilterTransactionTable(String tableName) {
        when(gtidLogEvent.getServerUUID()).thenReturn(UUID.fromString(UUID_2));
        when(gtidLogEvent.getGtid()).thenReturn(GTID_2);
        when(gtidLogEvent.getLogEventHeader()).thenReturn(logEventHeader);
        when(gtidLogEvent.getLogEventType()).thenReturn(LogEventType.gtid_log_event);
        when(logEventHeader.getEventType()).thenReturn(LogEventType.gtid_log_event.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);

        logEventWithGroupFlag = new InboundLogEventContext(gtidLogEvent, null, new TransactionFlags(), StringUtils.EMPTY);
        boolean skip = flagFilterWithTT.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip);


        when(logEventHeader.getEventType()).thenReturn(LogEventType.query_log_event.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(queryLogEvent.getLogEventType()).thenReturn(LogEventType.query_log_event);
        when(queryLogEvent.getLogEventHeader()).thenReturn(logEventHeader);

        String ALTER_TABLE = "alter /* gh-ost */ table `ghostdb`.`_test1g_gho` ADD COLUMN addcol119 VARCHAR(255) DEFAULT NULL COMMENT '添加普通列测试'";
        when(queryLogEvent.getQuery()).thenReturn(ALTER_TABLE);

        logEventWithGroupFlag.setLogEvent(queryLogEvent);
        skip = flagFilterWithTT.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip);

        when(logEventHeader.getEventType()).thenReturn(LogEventType.table_map_log_event.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(tableMapLogEvent.getLogEventType()).thenReturn(LogEventType.table_map_log_event);
        when(tableMapLogEvent.getTableName()).thenReturn(tableName);
        when(tableMapLogEvent.getLogEventHeader()).thenReturn(logEventHeader);

        logEventWithGroupFlag.setLogEvent(tableMapLogEvent);
        skip = flagFilterWithTT.doFilter(logEventWithGroupFlag);
        boolean expectedSkip = DRC_TRANSACTION_TABLE_NAME.equalsIgnoreCase(tableName) ? true : false;

        Assert.assertEquals(skip, expectedSkip);

        when(logEventHeader.getEventType()).thenReturn(LogEventType.write_rows_event_v2.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(writeRowsEvent.getLogEventType()).thenReturn(LogEventType.write_rows_event_v2);
        when(writeRowsEvent.getLogEventHeader()).thenReturn(logEventHeader);

        logEventWithGroupFlag.setLogEvent(writeRowsEvent);
        skip = flagFilterWithTT.doFilter(logEventWithGroupFlag);
        Assert.assertEquals(skip, expectedSkip);

        when(logEventHeader.getEventType()).thenReturn(LogEventType.xid_log_event.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(xidLogEvent.getLogEventType()).thenReturn(LogEventType.xid_log_event);
        when(xidLogEvent.getLogEventHeader()).thenReturn(logEventHeader);

        logEventWithGroupFlag.setLogEvent(xidLogEvent);
        skip = flagFilterWithTT.doFilter(logEventWithGroupFlag);
        Assert.assertEquals(skip, expectedSkip);
    }

    @Test
    public void testFilterGtidTrue() {
        when(gtidLogEvent.getServerUUID()).thenReturn(UUID.fromString(UUID_2));
        when(gtidLogEvent.getGtid()).thenReturn(GTID);
        when(gtidLogEvent.getLogEventHeader()).thenReturn(logEventHeader);
        when(gtidLogEvent.getLogEventType()).thenReturn(LogEventType.gtid_log_event);
        when(logEventHeader.getEventType()).thenReturn(LogEventType.gtid_log_event.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);

        logEventWithGroupFlag = new InboundLogEventContext(gtidLogEvent,  null,new TransactionFlags(), StringUtils.EMPTY);
        boolean skip = flagFilterWithGN.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);

        when(logEventHeader.getEventType()).thenReturn(LogEventType.query_log_event.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(queryLogEvent.getLogEventType()).thenReturn(LogEventType.query_log_event);
        when(queryLogEvent.getLogEventHeader()).thenReturn(logEventHeader);

        String ALTER_TABLE = "alter /* gh-ost */ table `ghostdb`.`_test1g_gho` ADD COLUMN addcol119 VARCHAR(255) DEFAULT NULL COMMENT '添加普通列测试'";
        when(queryLogEvent.getQuery()).thenReturn(ALTER_TABLE);

        logEventWithGroupFlag.setLogEvent(queryLogEvent);
        skip = flagFilterWithGN.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);

        when(logEventHeader.getEventType()).thenReturn(LogEventType.table_map_log_event.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(tableMapLogEvent.getLogEventType()).thenReturn(LogEventType.table_map_log_event);
        when(tableMapLogEvent.getLogEventHeader()).thenReturn(logEventHeader);

        logEventWithGroupFlag.setLogEvent(tableMapLogEvent);
        skip = flagFilterWithGN.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);

        when(logEventHeader.getEventType()).thenReturn(LogEventType.write_rows_event_v2.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(writeRowsEvent.getLogEventType()).thenReturn(LogEventType.write_rows_event_v2);
        when(writeRowsEvent.getLogEventHeader()).thenReturn(logEventHeader);

        logEventWithGroupFlag.setLogEvent(writeRowsEvent);
        skip = flagFilterWithGN.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);

        when(logEventHeader.getEventType()).thenReturn(LogEventType.xid_log_event.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(xidLogEvent.getLogEventType()).thenReturn(LogEventType.xid_log_event);
        when(xidLogEvent.getLogEventHeader()).thenReturn(logEventHeader);

        logEventWithGroupFlag.setLogEvent(xidLogEvent);
        skip = flagFilterWithGN.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);
    }

    @Test
    public void testFilterBlackTableName() {
        doTestFilterBlackTableName(flagFilterWithGN);
        doTestFilterBlackTableName(flagFilterWithTT);
    }

    public void doTestFilterBlackTableName(Filter<InboundLogEventContext> flagFilter) {
        when(gtidLogEvent.getServerUUID()).thenReturn(UUID.fromString(UUID_1));
        when(gtidLogEvent.getGtid()).thenReturn(GTID);
        when(gtidLogEvent.getLogEventHeader()).thenReturn(logEventHeader);
        when(gtidLogEvent.getLogEventType()).thenReturn(LogEventType.gtid_log_event);
        when(logEventHeader.getEventType()).thenReturn(LogEventType.gtid_log_event.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);

        logEventWithGroupFlag = new InboundLogEventContext(gtidLogEvent, null, new TransactionFlags(), GTID);
        boolean skip = flagFilter.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip);

        when(queryLogEvent.getLogEventType()).thenReturn(LogEventType.query_log_event);
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(queryLogEvent.getLogEventType()).thenReturn(LogEventType.query_log_event);
        when(queryLogEvent.getLogEventHeader()).thenReturn(logEventHeader);

        String ALTER_TABLE = "alter /* gh-ost */ table `ghostdb`.`_test1g_gho` ADD COLUMN addcol119 VARCHAR(255) DEFAULT NULL COMMENT '添加普通列测试'";
        when(queryLogEvent.getQuery()).thenReturn(ALTER_TABLE);

        logEventWithGroupFlag.setLogEvent(queryLogEvent);
        skip = flagFilter.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip);

        when(tableMapLogEvent.getLogEventType()).thenReturn(LogEventType.table_map_log_event);
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(tableMapLogEvent.getLogEventType()).thenReturn(LogEventType.table_map_log_event);
        when(tableMapLogEvent.getLogEventHeader()).thenReturn(logEventHeader);
        when(tableMapLogEvent.getSchemaName()).thenReturn("configdb");
        when(tableMapLogEvent.getSchemaNameDotTableName()).thenReturn("configdb.healthcheck");

        logEventWithGroupFlag.setLogEvent(tableMapLogEvent);
        skip = flagFilter.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);

        when(logEventHeader.getEventType()).thenReturn(LogEventType.write_rows_event_v2.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(writeRowsEvent.getLogEventType()).thenReturn(LogEventType.write_rows_event_v2);
        when(writeRowsEvent.getLogEventHeader()).thenReturn(logEventHeader);

        logEventWithGroupFlag.setLogEvent(writeRowsEvent);
        skip = flagFilter.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);

        // the previous event was filtered, this event should not be filtered
        when(tableMapLogEvent.getLogEventType()).thenReturn(LogEventType.table_map_log_event);
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(tableMapLogEvent.getLogEventType()).thenReturn(LogEventType.table_map_log_event);
        when(tableMapLogEvent.getLogEventHeader()).thenReturn(logEventHeader);
        when(tableMapLogEvent.getSchemaName()).thenReturn("testdb");
        when(tableMapLogEvent.getSchemaNameDotTableName()).thenReturn("testdb.testtable");

        logEventWithGroupFlag.setLogEvent(tableMapLogEvent);
        skip = flagFilter.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip);

        when(logEventHeader.getEventType()).thenReturn(LogEventType.write_rows_event_v2.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(writeRowsEvent.getLogEventType()).thenReturn(LogEventType.write_rows_event_v2);
        when(writeRowsEvent.getLogEventHeader()).thenReturn(logEventHeader);

        logEventWithGroupFlag.setLogEvent(writeRowsEvent);
        skip = flagFilter.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip);

        when(logEventHeader.getEventType()).thenReturn(LogEventType.xid_log_event.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(xidLogEvent.getLogEventType()).thenReturn(LogEventType.xid_log_event);
        when(xidLogEvent.getLogEventHeader()).thenReturn(logEventHeader);

        logEventWithGroupFlag.setLogEvent(xidLogEvent);
        skip = flagFilter.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip);
    }

    @Test
    public void testResetAfterEachGtidEvent() {
        doTestResetAfterEachGtidEvent(flagFilterWithGN);
        doTestResetAfterEachGtidEvent(flagFilterWithTT);
    }

    public void doTestResetAfterEachGtidEvent(Filter<InboundLogEventContext> flagFilter) {
        when(gtidLogEvent.getServerUUID()).thenReturn(UUID.fromString(UUID_1));
        when(gtidLogEvent.getGtid()).thenReturn(GTID);
        when(gtidLogEvent.getLogEventHeader()).thenReturn(logEventHeader);
        when(gtidLogEvent.getLogEventType()).thenReturn(LogEventType.gtid_log_event);
        when(logEventHeader.getEventType()).thenReturn(LogEventType.gtid_log_event.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);

        // reset GTID_F
        TransactionFlags setGtidFlag = new TransactionFlags();
        setGtidFlag.mark(GTID_F);
        logEventWithGroupFlag = new InboundLogEventContext(gtidLogEvent, null, setGtidFlag, GTID);
        boolean skip1 = flagFilter.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip1);
        Assert.assertFalse(setGtidFlag.filtered());

        // reset TRANSACTION_TABLE_F
        TransactionFlags transactionTableFlag = new TransactionFlags();
        transactionTableFlag.mark(TRANSACTION_TABLE_F);
        logEventWithGroupFlag = new InboundLogEventContext(gtidLogEvent, null, transactionTableFlag, GTID);
        boolean skip2 = flagFilter.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip2);
        Assert.assertFalse(transactionTableFlag.filtered());

        // reset BLACK_TABLE_NAME_F
        TransactionFlags blackTableFlag = new TransactionFlags();
        blackTableFlag.mark(BLACK_TABLE_NAME_F);
        logEventWithGroupFlag = new InboundLogEventContext(gtidLogEvent, null, blackTableFlag, GTID);
        boolean skip3 = flagFilter.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip3);
        Assert.assertFalse(blackTableFlag.filtered());

        // reset OTHER_F
        TransactionFlags otherFlag = new TransactionFlags();
        otherFlag.mark(OTHER_F);
        logEventWithGroupFlag = new InboundLogEventContext(gtidLogEvent, null, otherFlag, GTID);
        boolean skip4 = flagFilter.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip4);
        Assert.assertFalse(otherFlag.filtered());
    }
}
