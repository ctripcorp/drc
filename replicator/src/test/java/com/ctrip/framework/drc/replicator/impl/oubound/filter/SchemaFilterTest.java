package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.impl.FilterLogEvent;
import com.ctrip.framework.drc.core.driver.command.packet.applier.ApplierDumpCommandPacket;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.BinlogScanner;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.scanner.ScannerFilterChainContext;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.scanner.ScannerSchemaFilter;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.binlog.LocalBinlogSender;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Set;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.drc_filter_log_event;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * Created by jixinwang on 2023/11/21
 */
public class SchemaFilterTest {

    @Test
    public void doFilter() throws IOException {
        ScannerFilterChainContext context = new ScannerFilterChainContext();
        context.setRegisterKey("testKey");
        BinlogScanner scanner = mock(BinlogScanner.class);
        String nameFilter = "drc1\\.insert1,drc2.insert2";
        ApplierDumpCommandPacket packet = new ApplierDumpCommandPacket("test", new GtidSet("abc:123"));
        packet.setNameFilter(nameFilter);
        when(scanner.getSenders()).thenReturn(Lists.newArrayList(new LocalBinlogSender(null, packet)));
        context.setScanner(scanner);
        ScannerSchemaFilter schemaFilter = new ScannerSchemaFilter(context);
        Set<String> schemas = ScannerSchemaFilter.getSchemas(nameFilter);

        Assert.assertEquals(4, schemas.size());
        Assert.assertTrue(schemas.contains("drc1"));
        Assert.assertTrue(schemas.contains("drc2"));

        OutboundLogEventContext value = new OutboundLogEventContext();
        FileChannel fileChannel = mock(FileChannel.class);
        value.setFileChannel(fileChannel);
        value.setEventType(drc_filter_log_event);
        FilterLogEvent filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("drc1", FilterLogEvent.UNKNOWN, 10, 10, 1);
        value.setLogEvent(filterLogEvent);
        schemaFilter.doFilter(value);
        Mockito.verify(fileChannel, times(0)).position();

        filterLogEvent.encode("drc3", FilterLogEvent.UNKNOWN, 10, 10, 1);
        schemaFilter.doFilter(value);
        Mockito.verify(fileChannel, times(1)).position(anyLong());
    }
}