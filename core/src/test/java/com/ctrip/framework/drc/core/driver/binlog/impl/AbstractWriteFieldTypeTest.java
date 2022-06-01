package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import org.apache.tomcat.util.buf.HexUtils;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;

import static com.ctrip.framework.drc.core.server.utils.RowsEventUtils.transformMetaAndType;

/**
 * Created by jixinwang on 2022/5/17
 */
public abstract class AbstractWriteFieldTypeTest {

    protected WriteRowsEvent writeRowsEvent;

    protected TableMapLogEvent tableMapLogEvent;

    protected TableMapLogEvent drcTableMapLogEvent;

    protected Columns columns;

    @Before
    public void setUp() throws Exception {
        ByteBuf tByteBuf = tableMapEventForWriteRowsEvent();
        tableMapLogEvent = new TableMapLogEvent().read(tByteBuf);
        drcTableMapLogEvent = getDrcTableMapEvent();
        Columns originColumns = Columns.from(tableMapLogEvent.getColumns());
        columns = Columns.from(drcTableMapLogEvent.getColumns());
        transformMetaAndType(originColumns, columns);
    }

    protected void testWriteValue(String rowsHexString, boolean hasExpected, String expected) throws IOException {
        ByteBuf wByteBuf = writeRowsEvent(rowsHexString);
        writeRowsEvent = new WriteRowsEvent().read(wByteBuf);
        writeRowsEvent.load(columns);
        String actual = writeRowsEvent.getRows().get(0).getBeforeValues().get(0).toString();
//        System.out.println("actual row value is: " + actual);
        if (hasExpected) {
            Assert.assertEquals(expected, actual);
        }
        String oldPayloadBuf = ByteBufUtil.hexDump(writeRowsEvent.getPayloadBuf().resetReaderIndex());
        WriteRowsEvent newWriteRowsEvent1 = new WriteRowsEvent(writeRowsEvent, columns);
        String newPayloadBuf = ByteBufUtil.hexDump(newWriteRowsEvent1.getPayloadBuf().resetReaderIndex());
        Assert.assertEquals(oldPayloadBuf, newPayloadBuf);
    }

    protected void testWriteValue(String rowsHexString, String expected) throws IOException {
        testWriteValue(rowsHexString, true, expected);
    }

    protected void testWriteValue(String rowsHexString) throws IOException {
        testWriteValue(rowsHexString, false, null);
    }

    protected ByteBuf tableMapEventForWriteRowsEvent() {
        String hexString = getTableMapEventHexString();
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

    protected ByteBuf writeRowsEvent(String rowsHexString) {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
        final byte[] bytes = toBytesFromHexString(rowsHexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    protected abstract TableMapLogEvent getDrcTableMapEvent() throws IOException;

    protected abstract String getTableMapEventHexString();

}
