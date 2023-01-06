package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.monitor.kpi.OutboundMonitorReport;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.enums.RowsFilterType;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.extract.ExtractFilter;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.GenericFutureListener;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;
import static com.ctrip.framework.drc.core.server.common.filter.row.RuleFactory.ROWS_FILTER_RULE;
import static com.ctrip.framework.drc.replicator.AllTests.ROW_FILTER_PROPERTIES;

/**
 * @Author limingdong
 * @create 2022/4/28
 */
public class OutboundFilterChainFactoryTest extends AbstractRowsFilterTest {

    private OutboundFilterChainFactory filterChainFactory;

    private OutboundFilterChainContext filterChainContext;

    @Mock
    private Channel channel;

    @Mock
    private ChannelFuture channelFuture;

    @Mock
    private OutboundMonitorReport outboundMonitorReport;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        when(channel.writeAndFlush(any(ByteBuf.class))).thenReturn(channelFuture);
        String properties = String.format(ROW_FILTER_PROPERTIES, RowsFilterType.Custom.getName());
        filterChainContext= new OutboundFilterChainContext(channel, ConsumeType.Applier, DataMediaConfig.from("ut_test", properties), outboundMonitorReport);
        filterChainFactory = new OutboundFilterChainFactory();
    }

    @Override
    protected RowsFilterType getRowsFilterType() {
        System.setProperty(ROWS_FILTER_RULE, "com.ctrip.framework.drc.replicator.impl.oubound.filter.CustomRowsFilterRule");
        return RowsFilterType.Custom;
    }

    @Test
    public void createFilterChain() {
        Filter<OutboundLogEventContext> filter = filterChainFactory.createFilterChain(filterChainContext);
        Filter first = filter;
        Assert.assertTrue(filter instanceof SendFilter);
        filter = filter.getSuccessor();
        Assert.assertTrue(filter instanceof TypeFilter);
        filter = filter.getSuccessor();
        Assert.assertTrue(filter instanceof TableFilter);
        filter = filter.getSuccessor();
        Assert.assertTrue(filter instanceof ExtractFilter);
        filter = filter.getSuccessor();
        Assert.assertNull(filter);
        first.release();
    }

    @Test
    public void testDoFilter() throws IOException {
        Filter<OutboundLogEventContext> filterChain = filterChainFactory.createFilterChain(filterChainContext);

        // drc_table_map_log_event
        TableMapLogEvent tableMapLogEvent = drcTableMapEvent();
        String tableName = tableMapLogEvent.getSchemaNameDotTableName();
        ByteBuf byteBuf = tableMapLogEvent.getLogEventHeader().getHeaderBuf();
        int endIndex = byteBuf.writerIndex();
        ByteBuffer byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);

        byteBuf = tableMapLogEvent.getPayloadBuf();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);

        long previousPosition = 0;
        long currentPosition = fileChannel.position();
        fileChannel.position(previousPosition + eventHeaderLengthVersionGt1);

        logEventType = LogEventType.drc_table_map_log_event;
        outboundLogEventContext = new OutboundLogEventContext(fileChannel, previousPosition + eventHeaderLengthVersionGt1, logEventType, currentPosition - previousPosition, "");
        boolean noRowsFilter = filterChain.doFilter(outboundLogEventContext);
        Assert.assertTrue(noRowsFilter);

        fileChannel.position(currentPosition);
        previousPosition = currentPosition;

        // gtid_log_event
        byteBuf = getGtidEvent();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);

        logEventType = LogEventType.gtid_log_event;
        outboundLogEventContext = new OutboundLogEventContext(fileChannel, previousPosition + eventHeaderLengthVersionGt1, logEventType, currentPosition - previousPosition, "");
        noRowsFilter = filterChain.doFilter(outboundLogEventContext);
        Assert.assertTrue(noRowsFilter);

        fileChannel.position(currentPosition);
        previousPosition = currentPosition;

        // table_map_log_event
        byteBuf = tableMapEvent();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);

        logEventType = LogEventType.table_map_log_event;
        outboundLogEventContext = new OutboundLogEventContext(fileChannel, previousPosition + eventHeaderLengthVersionGt1, logEventType, currentPosition - previousPosition, "");
        noRowsFilter = filterChain.doFilter(outboundLogEventContext);
        Assert.assertTrue(noRowsFilter);

        fileChannel.position(currentPosition);
        previousPosition = currentPosition;

        // rows_log_event
        byteBuf = writeRowsEvent();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);

        currentPosition = fileChannel.position();
        fileChannel.position(previousPosition + eventHeaderLengthVersionGt1);

        logEventType = LogEventType.write_rows_event_v2;
        outboundLogEventContext = new OutboundLogEventContext(fileChannel, previousPosition + eventHeaderLengthVersionGt1, logEventType, currentPosition - previousPosition, "");
        noRowsFilter = filterChain.doFilter(outboundLogEventContext);
        Assert.assertFalse(noRowsFilter);
        Assert.assertNotNull(outboundLogEventContext.getDrcTableMap(tableName));
        Assert.assertNotNull(outboundLogEventContext.getTableMapWithinTransaction(table_id));

        fileChannel.position(currentPosition);
        previousPosition = currentPosition;

        // rows_log_event
        byteBuf = writeRowsEvent();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);

        currentPosition = fileChannel.position();
        fileChannel.position(previousPosition + eventHeaderLengthVersionGt1);

        logEventType = LogEventType.write_rows_event_v2;
        outboundLogEventContext = new OutboundLogEventContext(fileChannel, previousPosition + eventHeaderLengthVersionGt1, logEventType, currentPosition - previousPosition, "");
        noRowsFilter = filterChain.doFilter(outboundLogEventContext);
        Assert.assertFalse(noRowsFilter);
        Assert.assertNotNull(outboundLogEventContext.getDrcTableMap(tableName));
        Assert.assertNotNull(outboundLogEventContext.getTableMapWithinTransaction(table_id));
        verify(channelFuture, times(2)).addListener(any(GenericFutureListener.class)); // event

        fileChannel.position(currentPosition);
        previousPosition = currentPosition;

        // rows_log_event
        byteBuf = writeRowsEvent();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);

        currentPosition = fileChannel.position();
        fileChannel.position(previousPosition + eventHeaderLengthVersionGt1);

        logEventType = LogEventType.write_rows_event_v2;
        outboundLogEventContext = new OutboundLogEventContext(fileChannel, previousPosition + eventHeaderLengthVersionGt1, logEventType, currentPosition - previousPosition, "");
        noRowsFilter = filterChain.doFilter(outboundLogEventContext);
        Assert.assertFalse(noRowsFilter);
        Assert.assertNotNull(outboundLogEventContext.getDrcTableMap(tableName));
        Assert.assertNotNull(outboundLogEventContext.getTableMapWithinTransaction(table_id));
        verify(channelFuture, times(3)).addListener(any(GenericFutureListener.class)); // event

        fileChannel.position(currentPosition);
        previousPosition = currentPosition;

        // xid_log_event
        byteBuf = getXidEvent();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);

        currentPosition = fileChannel.position();
        fileChannel.position(previousPosition + eventHeaderLengthVersionGt1);

        logEventType = LogEventType.xid_log_event;
        outboundLogEventContext = new OutboundLogEventContext(fileChannel, previousPosition + eventHeaderLengthVersionGt1, logEventType, currentPosition - previousPosition, "");
        noRowsFilter = filterChain.doFilter(outboundLogEventContext);
        Assert.assertTrue(noRowsFilter);

        Assert.assertNull(outboundLogEventContext.getDrcTableMap(tableName));
        Assert.assertNull(outboundLogEventContext.getTableMapWithinTransaction(table_id));

        // test release
        Filter successor = filterChain.getSuccessor();
        while (successor != null) {
            if (successor instanceof TableFilter) {
                TableFilter filter = (TableFilter) successor;
                Assert.assertFalse(filter.getDrcTableMap().isEmpty());
                filterChain.release();
                Assert.assertTrue(filter.getDrcTableMap().isEmpty());
            }
            successor = successor.getSuccessor();
        }
    }

    @Test
    public void testDoFilter_columnsFilter() throws Exception {
        doTestDoFilter_columnsFilter(COLUMNS_FILTER_PROPERTIES_EXCLUDE);
        doTestDoFilter_columnsFilter(COLUMNS_FILTER_PROPERTIES_EXCLUDE_MIX_CASE);
        doTestDoFilter_columnsFilter(COLUMNS_FILTER_PROPERTIES_INCLUDE);
        doTestDoFilter_columnsFilter(COLUMNS_FILTER_PROPERTIES_INCLUDE_MIX_CASE);
    }

    public void doTestDoFilter_columnsFilter(String columnsFilterProperty) throws Exception {
        filterChainContext= new OutboundFilterChainContext(channel, ConsumeType.Applier, DataMediaConfig.from("ut_test", columnsFilterProperty), outboundMonitorReport);
        filterChainFactory = new OutboundFilterChainFactory();


        Filter<OutboundLogEventContext> filterChain = filterChainFactory.createFilterChain(filterChainContext);

        // drc_table_map_log_event
        TableMapLogEvent tableMapLogEvent = drcTableMapEvent();
        String tableName = tableMapLogEvent.getSchemaNameDotTableName();
        ByteBuf byteBuf = tableMapLogEvent.getLogEventHeader().getHeaderBuf();
        int endIndex = byteBuf.writerIndex();
        ByteBuffer byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);

        byteBuf = tableMapLogEvent.getPayloadBuf();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);

        long previousPosition = 0;
        long currentPosition = fileChannel.position();
        fileChannel.position(previousPosition + eventHeaderLengthVersionGt1);

        logEventType = LogEventType.drc_table_map_log_event;
        outboundLogEventContext = new OutboundLogEventContext(fileChannel, previousPosition + eventHeaderLengthVersionGt1, logEventType, currentPosition - previousPosition, "");
        boolean noRewrite = filterChain.doFilter(outboundLogEventContext);
        Assert.assertFalse(noRewrite);

        fileChannel.position(currentPosition);
        previousPosition = currentPosition;

        // gtid_log_event
        byteBuf = getGtidEvent();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);

        logEventType = LogEventType.gtid_log_event;
        outboundLogEventContext = new OutboundLogEventContext(fileChannel, previousPosition + eventHeaderLengthVersionGt1, logEventType, currentPosition - previousPosition, "");
        noRewrite = filterChain.doFilter(outboundLogEventContext);
        Assert.assertTrue(noRewrite);

        fileChannel.position(currentPosition);
        previousPosition = currentPosition;

        // table_map_log_event
        byteBuf = tableMapEvent();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);

        logEventType = LogEventType.table_map_log_event;
        outboundLogEventContext = new OutboundLogEventContext(fileChannel, previousPosition + eventHeaderLengthVersionGt1, logEventType, currentPosition - previousPosition, "");
        noRewrite = filterChain.doFilter(outboundLogEventContext);
        Assert.assertFalse(noRewrite);

        fileChannel.position(currentPosition);
        previousPosition = currentPosition;

        // rows_log_event
        byteBuf = writeRowsEvent();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);

        currentPosition = fileChannel.position();
        fileChannel.position(previousPosition + eventHeaderLengthVersionGt1);

        logEventType = LogEventType.write_rows_event_v2;
        outboundLogEventContext = new OutboundLogEventContext(fileChannel, previousPosition + eventHeaderLengthVersionGt1, logEventType, currentPosition - previousPosition, "");
        noRewrite = filterChain.doFilter(outboundLogEventContext);
        Assert.assertFalse(noRewrite);
        Assert.assertNotNull(outboundLogEventContext.getDrcTableMap(tableName));
        Assert.assertNotNull(outboundLogEventContext.getTableMapWithinTransaction(table_id));

        fileChannel.position(currentPosition);
        previousPosition = currentPosition;

        // rows_log_event
        byteBuf = writeRowsEvent();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);

        currentPosition = fileChannel.position();
        fileChannel.position(previousPosition + eventHeaderLengthVersionGt1);

        logEventType = LogEventType.write_rows_event_v2;
        outboundLogEventContext = new OutboundLogEventContext(fileChannel, previousPosition + eventHeaderLengthVersionGt1, logEventType, currentPosition - previousPosition, "");
        noRewrite = filterChain.doFilter(outboundLogEventContext);
        Assert.assertFalse(noRewrite);
        Assert.assertNotNull(outboundLogEventContext.getDrcTableMap(tableName));
        Assert.assertNotNull(outboundLogEventContext.getTableMapWithinTransaction(table_id));

        fileChannel.position(currentPosition);
        previousPosition = currentPosition;

        // rows_log_event
        byteBuf = writeRowsEvent();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);

        currentPosition = fileChannel.position();
        fileChannel.position(previousPosition + eventHeaderLengthVersionGt1);

        logEventType = LogEventType.write_rows_event_v2;
        outboundLogEventContext = new OutboundLogEventContext(fileChannel, previousPosition + eventHeaderLengthVersionGt1, logEventType, currentPosition - previousPosition, "");
        noRewrite = filterChain.doFilter(outboundLogEventContext);
        Assert.assertFalse(noRewrite);
        Assert.assertNotNull(outboundLogEventContext.getDrcTableMap(tableName));
        Assert.assertNotNull(outboundLogEventContext.getTableMapWithinTransaction(table_id));

        fileChannel.position(currentPosition);
        previousPosition = currentPosition;

        // xid_log_event
        byteBuf = getXidEvent();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);

        currentPosition = fileChannel.position();
        fileChannel.position(previousPosition + eventHeaderLengthVersionGt1);

        logEventType = LogEventType.xid_log_event;
        outboundLogEventContext = new OutboundLogEventContext(fileChannel, previousPosition + eventHeaderLengthVersionGt1, logEventType, currentPosition - previousPosition, "");
        noRewrite = filterChain.doFilter(outboundLogEventContext);
        Assert.assertTrue(noRewrite);

        Assert.assertNull(outboundLogEventContext.getDrcTableMap(tableName));
        Assert.assertNull(outboundLogEventContext.getTableMapWithinTransaction(table_id));

        // test release
        Filter successor = filterChain.getSuccessor();
        while (successor != null) {
            if (successor instanceof TableFilter) {
                TableFilter filter = (TableFilter) successor;
                Assert.assertFalse(filter.getDrcTableMap().isEmpty());
                filterChain.release();
                Assert.assertTrue(filter.getDrcTableMap().isEmpty());
            }
            successor = successor.getSuccessor();
        }
    }

    private String COLUMNS_FILTER_PROPERTIES_EXCLUDE = "{" +
            "  \"columnsFilters\": [" +
            "    {" +
            "      \"mode\": \"exclude\"," +
            "      \"tables\": \"drc1.insert1\"," +
            "        \"columns\": [" +
            "          \"one\"" +
            "        ]" +
            "    }" +
            "  ]" +
            "}";

    private String COLUMNS_FILTER_PROPERTIES_EXCLUDE_MIX_CASE = "{" +
            "  \"columnsFilters\": [" +
            "    {" +
            "      \"mode\": \"exclude\"," +
            "      \"tables\": \"drc1.insert1\"," +
            "        \"columns\": [" +
            "          \"one\"," +
            "          \"THREe\"" +
            "        ]" +
            "    }" +
            "  ]" +
            "}";

    private String COLUMNS_FILTER_PROPERTIES_INCLUDE = "{" +
            "  \"columnsFilters\": [" +
            "    {" +
            "      \"mode\": \"include\"," +
            "      \"tables\": \"drc1.insert1\"," +
            "        \"columns\": [" +
            "          \"id\"," +
            "          \"datachange_lasttime\"" +
            "        ]" +
            "    }" +
            "  ]" +
            "}";

    private String COLUMNS_FILTER_PROPERTIES_INCLUDE_MIX_CASE = "{" +
            "  \"columnsFilters\": [" +
            "    {" +
            "      \"mode\": \"include\"," +
            "      \"tables\": \"drc1.insert1\"," +
            "        \"columns\": [" +
            "          \"id\"," +
            "          \"TWO\"" +
            "        ]" +
            "    }" +
            "  ]" +
            "}";
}
