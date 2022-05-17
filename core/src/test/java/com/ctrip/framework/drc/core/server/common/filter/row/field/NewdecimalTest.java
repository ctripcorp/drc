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
        ByteBuf tByteBuf = tableMapEventForWriteRowsEvent();
        tableMapLogEvent = new TableMapLogEvent().read(tByteBuf);
        drcTableMapLogEvent = drcTableMapEvent();
        Columns originColumns = Columns.from(tableMapLogEvent.getColumns());
        columns = Columns.from(drcTableMapLogEvent.getColumns());
        transformMetaAndType(originColumns, columns);
    }

    private void testDecimal(String rowsHexString) throws IOException {
        ByteBuf wByteBuf = writeRowsEvent(rowsHexString);
        writeRowsEvent = new WriteRowsEvent().read(wByteBuf);
        writeRowsEvent.load(columns);

        String oldPayloadBuf = ByteBufUtil.hexDump(writeRowsEvent.getPayloadBuf().resetReaderIndex());
        WriteRowsEvent newWriteRowsEvent1 = new WriteRowsEvent(writeRowsEvent, columns);
        String newPayloadBuf = ByteBufUtil.hexDump(newWriteRowsEvent1.getPayloadBuf().resetReaderIndex());
        Assert.assertEquals(oldPayloadBuf, newPayloadBuf);
    }

    // insert into drc1.newdecimal(amount) values(123.456);
    @Test
    public void testPositive() throws IOException {
        String rowsHexString = "f1 62 82 62   1e   ea 0c 00 00   2d 00 00 00   8c 01 00 00   00 00" +
                "6c 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 00" +
                "00 00 7b 06 f5 40 49 8a  aa 65";
        testDecimal(rowsHexString);
    }

    // insert into drc1.newdecimal(amount) values(-123.456);
    @Test
    public void testNonPositive() throws IOException {
        String rowsHexString = "f4 0e 83 62   1e   ea 0c 00 00   2d 00 00 00   99 02 00 00   00 00" +
                "6c 00 00 00 00 00 01 00  02 00 01 ff fe 7f ff ff" +
                "ff ff 84 f9 0a bf e4 66  3b 8a";
        testDecimal(rowsHexString);
    }

    // insert into drc1.newdecimal(amount) values(9999999999999.999999);
    @Test
    public void testMax() throws IOException {
        String rowsHexString = "63 1d 83 62   1e   ea 0c 00 00   2d 00 00 00   a6 03 00 00   00 00" +
                "6c 00 00 00 00 00 01 00  02 00 01 ff fe a7 0f 3b" +
                "9a c9 ff 0f 42 3f 49 5d  65 cd";
        testDecimal(rowsHexString);
    }

    // insert into drc1.newdecimal(amount) values(-9999999999999.999999);
    @Test
    public void testMin() throws IOException {
        String rowsHexString = "54 1e 83 62   1e   ea 0c 00 00   2d 00 00 00   b3 04 00 00   00 00" +
                "6c 00 00 00 00 00 01 00  02 00 01 ff fe 58 f0 c4" +
                "65 36 00 f0 bd c0 4c ce  fe 3e";
        testDecimal(rowsHexString);
    }

    // insert into drc1.newdecimal(amount) values(999999999.999);
    @Test
    public void testInt9() throws IOException {
        String rowsHexString = "7e 21 83 62   1e   ea 0c 00 00   2d 00 00 00   da 07 00 00   00 00" +
                "6c 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 3b" +
                "9a c9 ff 0f 3e 58 b7 9a  a7 6d";
        testDecimal(rowsHexString);
    }

    // insert into drc1.newdecimal(amount) values(0.1);
    @Test
    public void testScale1() throws IOException {
        String rowsHexString = "29 1f 83 62   1e   ea 0c 00 00   2d 00 00 00   c0 05 00 00   00 00" +
                "6c 00 00 00 00 00 01 00  02 00 01 ff fe 80 00 00" +
                "00 00 00 01 86 a0 44 91  ba ea";
        testDecimal(rowsHexString);
    }

    private ArrayList<TableMapLogEvent.Column> sourceColumns = Lists.newArrayList(
            new TableMapLogEvent.Column("amount", false, "decimal", null, "19", "6", null, null, null, "decimal(19,6)", null, null, null)
    );

    private List<List<String>> identifiers = Lists.<List<String>>newArrayList(
            Lists.<String>newArrayList()
    );

    protected TableMapLogEvent drcTableMapEvent() throws IOException {
        return new TableMapLogEvent(0, 0, 0, schemaName, tableName, sourceColumns, identifiers);
    }

    /*
     * CREATE DATABASE if not exists drc1;
     * CREATE TABLE `drc1`.`newdecimal` (
     *   `amount` decimal(19,6) NOT NULL COMMENT '金额'
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='newdecimal测试表';
     */
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

    protected ByteBuf writeRowsEvent(String rowsHexString) {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
        final byte[] bytes = toBytesFromHexString(rowsHexString);
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
