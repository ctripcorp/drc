package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.impl.FilterLogEvent;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Set;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.drc_filter_log_event;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.times;

/**
 * Created by jixinwang on 2023/11/21
 */
public class SchemaFilterTest {

    @Test
    public void doFilter() throws IOException {
        OutboundFilterChainContext context = new OutboundFilterChainContext();
        context.setRegisterKey("testKey");
        context.setNameFilter("drc1\\.insert1,drc2.insert2");
        SchemaFilter schemaFilter = new SchemaFilter(context);

        Set<String> schemas = schemaFilter.getSchemas();
        Assert.assertEquals(4, schemas.size());
        Assert.assertTrue(schemas.contains("drc1"));
        Assert.assertTrue(schemas.contains("drc2"));

        OutboundLogEventContext value = new OutboundLogEventContext();
        FileChannel fileChannel = Mockito.mock(FileChannel.class);
        value.setFileChannel(fileChannel);
        value.setEventType(drc_filter_log_event);
        FilterLogEvent filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("drc1", 1);
        value.setLogEvent(filterLogEvent);
        schemaFilter.doFilter(value);
        Mockito.verify(fileChannel, times(0)).position();

        filterLogEvent.encode("drc3", 1);
        schemaFilter.doFilter(value);
        Mockito.verify(fileChannel, times(1)).position(anyLong());
    }
}