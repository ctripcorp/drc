package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.binlog.impl.*;
import com.ctrip.framework.drc.core.driver.binlog.manager.SchemaManager;
import com.ctrip.framework.drc.core.monitor.kpi.InboundMonitorReport;
import com.ctrip.framework.drc.core.server.common.Filter;
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

/**
 * @Author limingdong
 * @create 2020/3/12
 */
public class FilterChainFactoryTest extends AbstractFilterTest {

    private static final String UUID_1 = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe";

    private static final String GTID = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1";

    private static final String CLUSTER_NAME = "test-drc";

    private static final String UUID_2 = "12027356-0d03-11ea-a2f0-c6a9fbf1c3fe";

    private FilterChainContext filterChainContext;

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

    private Filter<LogEventWithGroupFlag> flagFilter;

    private LogEventWithGroupFlag logEventWithGroupFlag;

    private DefaultMonitorManager delayMonitor = new DefaultMonitorManager();

    private TableFilterConfiguration tableFilterConfiguration = new TableFilterConfiguration();

    @Before
    public void setUp() throws Exception {
        super.initMocks();
        uuidSet.add(UUID.fromString(UUID_1));

        filterChainContext = new FilterChainContext(uuidSet, tableNames, schemaManager, inboundMonitorReport, transactionCache, delayMonitor, CLUSTER_NAME, tableFilterConfiguration);
        flagFilter = DefaultFilterChainFactory.createFilterChain(filterChainContext);
    }

    @Test
    public void testFilterGtidFalse() {
        when(gtidLogEvent.getServerUUID()).thenReturn(UUID.fromString(UUID_1));
        when(gtidLogEvent.getGtid()).thenReturn(GTID);
        when(gtidLogEvent.getLogEventHeader()).thenReturn(logEventHeader);
        when(gtidLogEvent.getLogEventType()).thenReturn(LogEventType.gtid_log_event);
        when(logEventHeader.getEventType()).thenReturn(LogEventType.gtid_log_event.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);

        logEventWithGroupFlag = new LogEventWithGroupFlag(gtidLogEvent, false, false, StringUtils.EMPTY);
        boolean skip = flagFilter.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip);


        when(logEventHeader.getEventType()).thenReturn(LogEventType.query_log_event.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(queryLogEvent.getLogEventType()).thenReturn(LogEventType.query_log_event);
        when(queryLogEvent.getLogEventHeader()).thenReturn(logEventHeader);

        String ALTER_TABLE = "alter /* gh-ost */ table `ghostdb`.`_test1g_gho` ADD COLUMN addcol119 VARCHAR(255) DEFAULT NULL COMMENT '添加普通列测试'";
        when(queryLogEvent.getQuery()).thenReturn(ALTER_TABLE);

        logEventWithGroupFlag.setInExcludeGroup(skip);
        logEventWithGroupFlag.setLogEvent(queryLogEvent);
        skip = flagFilter.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip);

        when(logEventHeader.getEventType()).thenReturn(LogEventType.table_map_log_event.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(tableMapLogEvent.getLogEventType()).thenReturn(LogEventType.table_map_log_event);
        when(tableMapLogEvent.getLogEventHeader()).thenReturn(logEventHeader);

        logEventWithGroupFlag.setInExcludeGroup(skip);
        logEventWithGroupFlag.setLogEvent(tableMapLogEvent);
        skip = flagFilter.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip);

        when(logEventHeader.getEventType()).thenReturn(LogEventType.write_rows_event_v2.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(writeRowsEvent.getLogEventType()).thenReturn(LogEventType.write_rows_event_v2);
        when(writeRowsEvent.getLogEventHeader()).thenReturn(logEventHeader);

        logEventWithGroupFlag.setInExcludeGroup(skip);
        logEventWithGroupFlag.setLogEvent(writeRowsEvent);
        skip = flagFilter.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip);

        when(logEventHeader.getEventType()).thenReturn(LogEventType.xid_log_event.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(xidLogEvent.getLogEventType()).thenReturn(LogEventType.xid_log_event);
        when(xidLogEvent.getLogEventHeader()).thenReturn(logEventHeader);

        logEventWithGroupFlag.setInExcludeGroup(skip);
        logEventWithGroupFlag.setLogEvent(xidLogEvent);
        skip = flagFilter.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip);
    }

    @Test
    public void testFilterGtidTrue() {
        when(gtidLogEvent.getServerUUID()).thenReturn(UUID.fromString(UUID_2));
        when(gtidLogEvent.getGtid()).thenReturn(GTID);
        when(gtidLogEvent.getLogEventHeader()).thenReturn(logEventHeader);
        when(gtidLogEvent.getLogEventType()).thenReturn(LogEventType.gtid_log_event);
        when(logEventHeader.getEventType()).thenReturn(LogEventType.gtid_log_event.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);

        logEventWithGroupFlag = new LogEventWithGroupFlag(gtidLogEvent, false, false, StringUtils.EMPTY);
        boolean skip = flagFilter.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);

        when(logEventHeader.getEventType()).thenReturn(LogEventType.query_log_event.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(queryLogEvent.getLogEventType()).thenReturn(LogEventType.query_log_event);
        when(queryLogEvent.getLogEventHeader()).thenReturn(logEventHeader);

        String ALTER_TABLE = "alter /* gh-ost */ table `ghostdb`.`_test1g_gho` ADD COLUMN addcol119 VARCHAR(255) DEFAULT NULL COMMENT '添加普通列测试'";
        when(queryLogEvent.getQuery()).thenReturn(ALTER_TABLE);

        logEventWithGroupFlag.setInExcludeGroup(skip);
        logEventWithGroupFlag.setLogEvent(queryLogEvent);
        skip = flagFilter.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);

        when(logEventHeader.getEventType()).thenReturn(LogEventType.table_map_log_event.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(tableMapLogEvent.getLogEventType()).thenReturn(LogEventType.table_map_log_event);
        when(tableMapLogEvent.getLogEventHeader()).thenReturn(logEventHeader);

        logEventWithGroupFlag.setInExcludeGroup(skip);
        logEventWithGroupFlag.setLogEvent(tableMapLogEvent);
        skip = flagFilter.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);

        when(logEventHeader.getEventType()).thenReturn(LogEventType.write_rows_event_v2.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(writeRowsEvent.getLogEventType()).thenReturn(LogEventType.write_rows_event_v2);
        when(writeRowsEvent.getLogEventHeader()).thenReturn(logEventHeader);

        logEventWithGroupFlag.setInExcludeGroup(skip);
        logEventWithGroupFlag.setLogEvent(writeRowsEvent);
        skip = flagFilter.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);

        when(logEventHeader.getEventType()).thenReturn(LogEventType.xid_log_event.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(xidLogEvent.getLogEventType()).thenReturn(LogEventType.xid_log_event);
        when(xidLogEvent.getLogEventHeader()).thenReturn(logEventHeader);

        logEventWithGroupFlag.setInExcludeGroup(skip);
        logEventWithGroupFlag.setLogEvent(xidLogEvent);
        skip = flagFilter.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);
    }

    @Test
    public void testFilterBlackTableName() {
        when(gtidLogEvent.getServerUUID()).thenReturn(UUID.fromString(UUID_1));
        when(gtidLogEvent.getGtid()).thenReturn(GTID);
        when(gtidLogEvent.getLogEventHeader()).thenReturn(logEventHeader);
        when(gtidLogEvent.getLogEventType()).thenReturn(LogEventType.gtid_log_event);
        when(logEventHeader.getEventType()).thenReturn(LogEventType.gtid_log_event.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);

        logEventWithGroupFlag = new LogEventWithGroupFlag(gtidLogEvent, false, false, GTID);
        boolean skip = flagFilter.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip);

        when(queryLogEvent.getLogEventType()).thenReturn(LogEventType.query_log_event);
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(queryLogEvent.getLogEventType()).thenReturn(LogEventType.query_log_event);
        when(queryLogEvent.getLogEventHeader()).thenReturn(logEventHeader);

        String ALTER_TABLE = "alter /* gh-ost */ table `ghostdb`.`_test1g_gho` ADD COLUMN addcol119 VARCHAR(255) DEFAULT NULL COMMENT '添加普通列测试'";
        when(queryLogEvent.getQuery()).thenReturn(ALTER_TABLE);

        logEventWithGroupFlag.setInExcludeGroup(skip);
        logEventWithGroupFlag.setLogEvent(queryLogEvent);
        skip = flagFilter.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip);

        when(tableMapLogEvent.getLogEventType()).thenReturn(LogEventType.table_map_log_event);
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(tableMapLogEvent.getLogEventType()).thenReturn(LogEventType.table_map_log_event);
        when(tableMapLogEvent.getLogEventHeader()).thenReturn(logEventHeader);
        when(tableMapLogEvent.getSchemaName()).thenReturn("configdb");
        when(tableMapLogEvent.getSchemaNameDotTableName()).thenReturn("configdb.healthcheck");

        logEventWithGroupFlag.setInExcludeGroup(skip);
        logEventWithGroupFlag.setLogEvent(tableMapLogEvent);
        skip = flagFilter.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);

        when(logEventHeader.getEventType()).thenReturn(LogEventType.write_rows_event_v2.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(writeRowsEvent.getLogEventType()).thenReturn(LogEventType.write_rows_event_v2);
        when(writeRowsEvent.getLogEventHeader()).thenReturn(logEventHeader);

        logEventWithGroupFlag.setInExcludeGroup(skip);
        logEventWithGroupFlag.setLogEvent(writeRowsEvent);
        skip = flagFilter.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);

        when(logEventHeader.getEventType()).thenReturn(LogEventType.xid_log_event.getType());
        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
        when(xidLogEvent.getLogEventType()).thenReturn(LogEventType.xid_log_event);
        when(xidLogEvent.getLogEventHeader()).thenReturn(logEventHeader);

        logEventWithGroupFlag.setInExcludeGroup(skip);
        logEventWithGroupFlag.setLogEvent(xidLogEvent);
        skip = flagFilter.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);
    }

}