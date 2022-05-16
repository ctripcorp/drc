package com.ctrip.framework.drc.core.server.common.filter.row.field;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.WriteRowsEvent;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import org.apache.tomcat.util.buf.HexUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.ctrip.framework.drc.core.meta.DataMediaConfig.from;
import static com.ctrip.framework.drc.core.server.utils.RowsEventUtils.transformMetaAndType;

/**
 * Created by jixinwang on 2022/5/16
 */
public class NewdecimalTest {

    protected WriteRowsEvent writeRowsEvent;

    protected TableMapLogEvent tableMapLogEvent;

    protected static final String schemaName = "drc1";

    protected static final String tableName = "newdecimal";

    protected TableMapLogEvent drcTableMapLogEvent;

    protected Columns columns;


    @Before
    public void setUp() throws Exception {
//        dataMediaConfig = from("registryKey", String.format(getProperties(), getRowsFilterType().getName(), getLocations()));
        ByteBuf tByteBuf = tableMapEventForWriteRowsEvent();
        tableMapLogEvent = new TableMapLogEvent().read(tByteBuf);
        ByteBuf wByteBuf = writeRowsEvent();
        writeRowsEvent = new WriteRowsEvent().read(wByteBuf);
        drcTableMapLogEvent = drcTableMapEvent();

        Columns originColumns = Columns.from(tableMapLogEvent.getColumns());
        columns = Columns.from(drcTableMapLogEvent.getColumns());
        transformMetaAndType(originColumns, columns);
        writeRowsEvent.load(columns);
    }

    @Test
    public void test() throws IOException {
        String oldPayloadBuf = ByteBufUtil.hexDump(writeRowsEvent.getPayloadBuf().resetReaderIndex());
        String oldHeaderBuf = ByteBufUtil.hexDump(writeRowsEvent.getLogEventHeader().getHeaderBuf().resetReaderIndex());
//        System.out.println("oldHeaderBuf: " + oldHeaderBuf);
//        System.out.println("oldPayloadBuf: " + oldPayloadBuf);

        WriteRowsEvent newWriteRowsEvent1 = new WriteRowsEvent(writeRowsEvent, columns);
        String newPayloadBuf = ByteBufUtil.hexDump(newWriteRowsEvent1.getPayloadBuf().resetReaderIndex());
        String newHeaderBuf = ByteBufUtil.hexDump(newWriteRowsEvent1.getLogEventHeader().getHeaderBuf().resetReaderIndex());
//        System.out.println("newHeaderBuf: " + newHeaderBuf);
//        System.out.println("newPayloadBuf: " + newPayloadBuf);
        Assert.assertEquals(oldPayloadBuf, newPayloadBuf);
    }

    private ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
            new TableMapLogEvent.Column("id", false, "decimal", null, "19", "6", null, null, null, "decimal(19,6)", null, null, null),
            new TableMapLogEvent.Column("one", true, "varchar", "90", null, null, null, "utf8", "utf8_general_ci", "varchar(30)", null, null, "one"),
            new TableMapLogEvent.Column("two", true, "varchar", "3000", null, null, null, "utf8", "utf8_general_ci", "varchar(1000)", null, null, "two"),
            new TableMapLogEvent.Column("three", true, "char", "90", null, null, null, "utf8", "utf8_general_ci", "char(30)", null, null, null),
            new TableMapLogEvent.Column("four", true, "char", "765", null, null, null, "utf8", "utf8_general_ci", "char(255)", null, null, null),
            new TableMapLogEvent.Column("datachange_lasttime", false, "timestamp", null, null, null, "3", null, null, "timestamp(3)", null, "on update CURRENT_TIMESTAMP(3)", "CURRENT_TIMESTAMP(3)")
    );

    private List<List<String>> identifiers = Lists.<List<String>>newArrayList(
            Lists.<String>newArrayList()
    );

    protected TableMapLogEvent drcTableMapEvent() throws IOException {
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }


    protected ByteBuf tableMapEventForWriteRowsEvent() {
        String hexString = "f1 62 82 62   13   ea 0c 00 00   37 00 00 00   5f 01 00 00   00 00" +
                           "6c 00 00 00 00 00 01 00  04 64 72 63 31 00 0a 6e" +
                           "65 77 64 65 63 69 6d 61  6c 00 01 f6 02 13 06 00" +
                           "db 59 4e 81";
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
        final byte[] bytes = toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    /*
     * insert into drc1.newdecimal(amount) values(123.456);
     */
    protected ByteBuf writeRowsEvent() {
        String hexString = "15f f1 62 82 62   1e   ea 0c 00 00   2d 00 00 00   8c 01 00 00   00 00" +
                           "172 6c 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 00" +
                           "182 00 00 7b 06 f5 40 49 8a  aa 65";
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
        final byte[] bytes = toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    public static byte[] toBytesFromHexString(String hexString) {
        // mac control+command+G multi rows operation
        hexString = hexString.replaceAll(" ", "");
        hexString = hexString.replaceAll("\n", "");
        return HexUtils.fromHexString(hexString);
    }
}
