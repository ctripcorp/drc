package com.ctrip.framework.drc.fetcher.event;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * @Author limingdong
 * @create 2021/3/24
 */
public class MonitoredDrcTableMapEventTest {

    @Mock
    MockLinkContext context;

    private MonitoredDrcTableMapEvent event;

    String schemaName = RandomStringUtils.randomAlphabetic(10);
    String tableName = RandomStringUtils.randomAlphabetic(10);
    ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
            new TableMapLogEvent.Column("id", true, "int", null, "10", null, null, null, null, "int(11)", null, null, null),
            new TableMapLogEvent.Column("user", true, "varchar", "60", "11", null, null, "utf8", "utf8_general_ci", "varchar(20)", null, null, null)
    );
    List<List<String>> identifiers = Lists.<List<String>>newArrayList(
            Lists.<String>newArrayList("id")
    );

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        event = spy(new MonitoredDrcTableMapEvent());
        when(event.getColumns()).thenReturn(Columns.from(sourceColumns));
        when(event.getSchemaName()).thenReturn(schemaName);
        when(event.getTableName()).thenReturn(tableName);
        when(event.getIdentifiers()).thenReturn(identifiers);
    }

    //MonitoredDrcTableMapEvent decodes schema name, table name their corresponding column metas
    // via its super class and update them to the link context.
    @Test
    public void asMetaEvent() throws Exception {
        doNothing().when(context).updateSchema(any(), any());
        event.involve(context);
        verify(context, times(1)).updateSchema(
                eq(TableKey.from(schemaName, tableName)),
                eq(Columns.from(sourceColumns, identifiers)));
    }

    @Test(expected = Exception.class)
    public void whenUpdateSchemaFail() throws Exception {
        doThrow(new RuntimeException("something goes wrong")).when(context).updateSchema(any(), any());
        event.involve(context);
    }

    @Test
    public void testReadEmptyIdentifiers() throws IOException {
        CompositeByteBuf compositeByteBuf = getTableMapLogEvent(null);

        MonitoredDrcTableMapEvent applierDrcTableMapEvent = new MonitoredDrcTableMapEvent();
        applierDrcTableMapEvent.read(compositeByteBuf);

        Assert.assertEquals(applierDrcTableMapEvent.getSchemaName(), schemaName);
        Assert.assertEquals(applierDrcTableMapEvent.getTableName(), tableName);
        Assert.assertEquals(applierDrcTableMapEvent.getColumns(), sourceColumns);
        Assert.assertEquals(applierDrcTableMapEvent.getIdentifiers(), Lists.newArrayList());
    }

    @Test
    public void testReadNonEmptyIdentifiers() throws IOException {
        List<List<String>> identifiers = Lists.newArrayList();
        List<String> pk = Lists.newArrayList("ID");
        List<String> uk = Lists.newArrayList("BasicOrderID", "Category");
        identifiers.add(pk);
        identifiers.add(uk);

        CompositeByteBuf compositeByteBuf = getTableMapLogEvent(identifiers);
        MonitoredDrcTableMapEvent applierDrcTableMapEvent = new MonitoredDrcTableMapEvent();
        applierDrcTableMapEvent.read(compositeByteBuf);

        Assert.assertEquals(applierDrcTableMapEvent.getSchemaName(), schemaName);
        Assert.assertEquals(applierDrcTableMapEvent.getTableName(), tableName);
        Assert.assertEquals(applierDrcTableMapEvent.getColumns(), sourceColumns);
        Assert.assertEquals(applierDrcTableMapEvent.getIdentifiers(), identifiers);
    }

    private CompositeByteBuf getTableMapLogEvent(List<List<String>> identifiers) throws IOException {
        TableMapLogEvent tableMapLogEvent = new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
        ByteBuf header = tableMapLogEvent.getLogEventHeader().getHeaderBuf();
        header.readerIndex(0);
        ByteBuf payload = tableMapLogEvent.getPayloadBuf();
        payload.readerIndex(0);
        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer();
        compositeByteBuf.addComponents(true, header, payload);
        return compositeByteBuf;
    }

}