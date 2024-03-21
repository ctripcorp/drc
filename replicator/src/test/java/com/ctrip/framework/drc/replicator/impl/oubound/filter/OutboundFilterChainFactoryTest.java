package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.monitor.kpi.OutboundMonitorReport;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.enums.RowsFilterType;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterContext;
import com.ctrip.framework.drc.replicator.impl.oubound.channel.ChannelAttributeKey;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.extract.ExtractFilter;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.GenericFutureListener;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.nio.ByteBuffer;

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

    @Mock
    private ChannelAttributeKey channelAttributeKey;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        when(channel.writeAndFlush(any(ByteBuf.class))).thenReturn(channelFuture);
        String properties = String.format(ROW_FILTER_PROPERTIES, RowsFilterType.Custom.getName());
        filterChainContext = getFilterChainContext(properties);
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
        Assert.assertTrue(filter instanceof MonitorFilter);
        filter = filter.getSuccessor();
        Assert.assertTrue(filter instanceof SendFilter);
        filter = filter.getSuccessor();
        Assert.assertTrue(filter instanceof ReadFilter);
        filter = filter.getSuccessor();
        Assert.assertTrue(filter instanceof IndexFilter);
        filter = filter.getSuccessor();
        Assert.assertTrue(filter instanceof SkipFilter);
        filter = filter.getSuccessor();
        Assert.assertTrue(filter instanceof TypeFilter);
        filter = filter.getSuccessor();
        Assert.assertTrue(filter instanceof SchemaFilter);
        filter = filter.getSuccessor();
        Assert.assertTrue(filter instanceof TableNameFilter);
        filter = filter.getSuccessor();
        Assert.assertTrue(filter instanceof ExtractFilter);
        filter = filter.getSuccessor();
        Assert.assertNull(filter);
        first.release();
    }

    @Test
    public void testDoFilter_rowsFilter() throws Exception {
        String properties = String.format(ROW_FILTER_PROPERTIES, RowsFilterType.Custom.getName());
        //2: 2writeRows, 3: 3writeRows
        testExtract(properties, 2, 3);
    }

    @Test
    public void testDoFilter_columnsFilter1() throws Exception {
        //4: 1drcTableMap + 1tableMap + 2writeRows, 5: 1drcTableMap + 1tableMap + 3writeRows
        testExtract(COLUMNS_FILTER_PROPERTIES_EXCLUDE, 4, 5);
    }

    @Test
    public void testDoFilter_columnsFilter2() throws Exception {
        //4: 1drcTableMap + 1tableMap + 2writeRows, 5: 1drcTableMap + 1tableMap + 3writeRows
        testExtract(COLUMNS_FILTER_PROPERTIES_EXCLUDE_MIX_CASE, 4, 5);
    }

    @Test
    public void testDoFilter_columnsFilter3() throws Exception {
        //4: 1drcTableMap + 1tableMap + 2writeRows, 5: 1drcTableMap + 1tableMap + 3writeRows
        testExtract(COLUMNS_FILTER_PROPERTIES_INCLUDE, 4, 5);
    }

    @Test
    public void testDoFilter_columnsFilter4() throws Exception {
        //4: 1drcTableMap + 1tableMap + 2writeRows, 5: 1drcTableMap + 1tableMap + 3writeRows
        testExtract(COLUMNS_FILTER_PROPERTIES_INCLUDE_MIX_CASE, 4, 5);
    }

    public void testExtract(String properties, int verify1, int verify2) throws Exception {
        filterChainContext = getFilterChainContext(properties);
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
        fileChannel.position(previousPosition);

        outboundLogEventContext = new OutboundLogEventContext(fileChannel, previousPosition, fileChannel.size());
        boolean skipEvent = filterChain.doFilter(outboundLogEventContext);
        Assert.assertFalse(skipEvent);

        // gtid_log_event
        byteBuf = getGtidEvent();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);
        previousPosition = currentPosition;
        currentPosition = fileChannel.position();
        fileChannel.position(previousPosition);

        outboundLogEventContext.reset(previousPosition, fileChannel.size());
        skipEvent = filterChain.doFilter(outboundLogEventContext);
        Assert.assertFalse(skipEvent);

        // table_map_log_event
        byteBuf = tableMapEvent();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);
        previousPosition = currentPosition;
        currentPosition = fileChannel.position();
        fileChannel.position(previousPosition);

        outboundLogEventContext.reset(previousPosition, fileChannel.size());
        skipEvent = filterChain.doFilter(outboundLogEventContext);
        Assert.assertFalse(skipEvent);

        // rows_log_event
        byteBuf = writeRowsEvent();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);
        previousPosition = currentPosition;
        currentPosition = fileChannel.position();
        fileChannel.position(previousPosition);

        outboundLogEventContext.reset(previousPosition, fileChannel.size());
        skipEvent = filterChain.doFilter(outboundLogEventContext);
        Assert.assertFalse(skipEvent);
        Assert.assertNotNull(outboundLogEventContext.getRowsRelatedTableMap().get(table_id));

        // rows_log_event
        byteBuf = writeRowsEvent();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);
        previousPosition = currentPosition;
        currentPosition = fileChannel.position();
        fileChannel.position(previousPosition);

        outboundLogEventContext.reset(previousPosition, fileChannel.size());
        skipEvent = filterChain.doFilter(outboundLogEventContext);
        Assert.assertFalse(skipEvent);
        Assert.assertNotNull(outboundLogEventContext.getRowsRelatedTableMap().get(table_id));
        verify(channelFuture, times(verify1)).addListener(any(GenericFutureListener.class)); // event

        // rows_log_event
        byteBuf = writeRowsEvent();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);
        previousPosition = currentPosition;
        currentPosition = fileChannel.position();
        fileChannel.position(previousPosition);

        outboundLogEventContext.reset(previousPosition, fileChannel.size());
        skipEvent = filterChain.doFilter(outboundLogEventContext);
        Assert.assertFalse(skipEvent);
        Assert.assertNotNull(outboundLogEventContext.getRowsRelatedTableMap().get(table_id));
        verify(channelFuture, times(verify2)).addListener(any(GenericFutureListener.class)); // event

        // xid_log_event
        byteBuf = getXidEvent();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);
        previousPosition = currentPosition;
        currentPosition = fileChannel.position();
        fileChannel.position(previousPosition);

        outboundLogEventContext.reset(previousPosition, fileChannel.size());
        skipEvent = filterChain.doFilter(outboundLogEventContext);
        Assert.assertFalse(skipEvent);
        Assert.assertNull(outboundLogEventContext.getRowsRelatedTableMap().get(table_id));

        // test release
        Filter successor = filterChain.getSuccessor();
        while (successor != null) {

            if (successor instanceof ReadFilter) {
                int refCnt = outboundLogEventContext.getCompositeByteBuf().refCnt();
                Assert.assertTrue(refCnt > 0);
                filterChain.release();
                refCnt = outboundLogEventContext.getCompositeByteBuf().refCnt();
                Assert.assertEquals(0, refCnt);
            }
            successor = successor.getSuccessor();
        }
    }

    private OutboundFilterChainContext getFilterChainContext(String properties) throws Exception {
        OutboundFilterChainContext filterChainContext = new OutboundFilterChainContext("test_key", channel, ConsumeType.Applier,
                DataMediaConfig.from("ut_test", properties), outboundMonitorReport, null,
                false, "drc1\\..*,drc2\\..*,drc3\\..*,drc4\\..*", null,
                channelAttributeKey, "src", "dst");
        return filterChainContext;
    }

    @Test
    public void testUdlRowsFilter() throws Exception {
        filterChainContext = getFilterChainContext(ROW_FILTER_UDL_PROPERTIES);
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
        fileChannel.position(previousPosition);

        outboundLogEventContext = new OutboundLogEventContext(fileChannel, previousPosition, fileChannel.size());
        boolean skipEvent = filterChain.doFilter(outboundLogEventContext);
        Assert.assertFalse(skipEvent);

        // gtid_log_event
        byteBuf = getGtidEvent();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);
        previousPosition = currentPosition;
        currentPosition = fileChannel.position();
        fileChannel.position(previousPosition);

        outboundLogEventContext.reset(previousPosition, fileChannel.size());
        skipEvent = filterChain.doFilter(outboundLogEventContext);
        Assert.assertFalse(skipEvent);

        // table_map_log_event
        byteBuf = tableMapEvent();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);
        previousPosition = currentPosition;
        currentPosition = fileChannel.position();
        fileChannel.position(previousPosition);

        outboundLogEventContext.reset(previousPosition, fileChannel.size());
        skipEvent = filterChain.doFilter(outboundLogEventContext);
        Assert.assertFalse(skipEvent);

        // rows_log_event
        byteBuf = writeRowsEvent();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);
        previousPosition = currentPosition;
        currentPosition = fileChannel.position();
        fileChannel.position(previousPosition);

        outboundLogEventContext.reset(previousPosition, fileChannel.size());
        skipEvent = filterChain.doFilter(outboundLogEventContext);
        Assert.assertFalse(skipEvent);
        Assert.assertNotNull(outboundLogEventContext.getRowsRelatedTableMap().get(table_id));

        Filter filter = filterChain.getSuccessor();
        while (filter != null) {
            if (filter instanceof ExtractFilter) {
                RowsFilterContext rowsFilterContext = ((ExtractFilter)filter).getExtractContext().getRowsFilterContext();
                Assert.assertTrue(rowsFilterContext.size() > 0);
                break;
            }
            filter = filter.getSuccessor();
        }

        // xid_log_event
        byteBuf = getXidEvent();
        endIndex = byteBuf.writerIndex();
        byteBuffer = byteBuf.internalNioBuffer(0, endIndex);
        fileChannel.write(byteBuffer);
        previousPosition = currentPosition;
        currentPosition = fileChannel.position();
        fileChannel.position(previousPosition);

        outboundLogEventContext.reset(previousPosition, fileChannel.size());
        skipEvent = filterChain.doFilter(outboundLogEventContext);
        Assert.assertFalse(skipEvent);
        Assert.assertNull(outboundLogEventContext.getRowsRelatedTableMap().get(table_id));

        Filter filter1 = filterChain.getSuccessor();
        while (filter1 != null) {
            if (filter1 instanceof ExtractFilter) {
                RowsFilterContext rowsFilterContext = ((ExtractFilter)filter1).getExtractContext().getRowsFilterContext();
                Assert.assertEquals(0, rowsFilterContext.size());
                break;
            }
            filter1 = filter1.getSuccessor();
        }

        // test release
        Filter successor = filterChain.getSuccessor();
        while (successor != null) {

            if (successor instanceof ReadFilter) {
                int refCnt = outboundLogEventContext.getCompositeByteBuf().refCnt();
                Assert.assertTrue(refCnt > 0);
                filterChain.release();
                refCnt = outboundLogEventContext.getCompositeByteBuf().refCnt();
                Assert.assertEquals(0, refCnt);
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

    private String ROW_FILTER_UDL_PROPERTIES = "{\n" +
            "  \"rowsFilters\": [\n" +
            "    {\n" +
            "      \"mode\": \"trip_udl\",\n" +
            "      \"tables\": \"drc1.insert1\",\n" +
            "      \"configs\": {\n" +
            "        \"parameterList\": [\n" +
            "          {\n" +
            "            \"columns\": [\n" +
            "              \"one\"\n" +
            "            ],\n" +
            "            \"illegalArgument\": true,\n" +
            "            \"fetchMode\": 0,\n" +
            "            \"userFilterMode\": \"udl\",\n" +
            "            \"drcStrategyId\": 1,\n" +
            "            \"context\": \"SHA\"\n" +
            "          }\n" +
            "        ]\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}";
}
