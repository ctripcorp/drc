package com.ctrip.framework.drc.core.driver.binlog.header;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEventTest;
import com.ctrip.framework.drc.core.driver.binlog.impl.WriteRowsEvent;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.write_rows_event_v2;

/**
 * Created by @author zhuYongMing on 2019/9/16.
 */
public class RowsEventPostHeaderTest {

    @Test
    public void readLazyTest() {
        // rowsEvent must be lazy load
        final ByteBuf byteBuf = initByteBuf();
        final WriteRowsEvent writeRowsEvent = new WriteRowsEvent().read(byteBuf);
        if (null == writeRowsEvent) {
            Assert.fail();
        }

        if (null == writeRowsEvent.getLogEventHeader()) {
            Assert.fail();
        }

        Assert.assertNull(writeRowsEvent.getRowsEventPostHeader());

        writeRowsEvent.load(TableMapLogEventTest.mockColumns());
        if (null == writeRowsEvent.getRowsEventPostHeader()) {
            Assert.fail();
        }

        // valid decode
        Assert.assertEquals(write_rows_event_v2, LogEventType.getLogEventType(writeRowsEvent.getLogEventHeader().getEventType()));
        Assert.assertEquals(123, writeRowsEvent.getRowsEventPostHeader().getTableId());
        Assert.assertEquals(1, writeRowsEvent.getRowsEventPostHeader().getFlags());
        Assert.assertEquals(2, writeRowsEvent.getRowsEventPostHeader().getExtraDataLength());
        Assert.assertNull(writeRowsEvent.getRowsEventPostHeader().getExtraData());

        Assert.assertEquals(49, byteBuf.readerIndex());
    }
    /**
     * binlog_row_image : minimal
     * insert into gtid_test.row_image3(`id`, `varchar`) values (9, 'varchar')
     *
     * # at 1676
     * #190915 23:49:37 server id 1  end_log_pos 1725 CRC32 0x80e874b8         Write_rows: table id 123 flags: STMT_END_F
     * ### INSERT INTO `gtid_test`.`row_image3`
     * ### SET
     * ###   @1=9 INT schema=0 nullable=1 is_null=0
     * ###   @4='varchar' VARSTRING(258) schema=258 nullable=1 is_null=0
     *
     * 0000068c  91 5d 7e 5d 1e 01 00 00  00 31 00 00 00 bd 06 00  |.]~].....1......|
     * 0000069c  00 00 00 7b 00 00 00 00  00 01 00 02 00 04 09 fc  |...{............|
     * 000006ac  09 00 00 00 07 00 76 61  72 63 68 61 72 b8 74 e8  |......varchar.t.|
     * 000006bc  80
     */
    private ByteBuf initByteBuf() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(49);
        byte[] bytes = new byte[] {
                (byte) 0x91, (byte) 0x5d, (byte) 0x7e, (byte) 0x5d, (byte) 0x1e, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x31, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xbd, (byte) 0x06, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x7b, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x02, (byte) 0x00, (byte) 0x04, (byte) 0x09, (byte) 0xfc,

                (byte) 0x09, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x07, (byte) 0x00, (byte) 0x76, (byte) 0x61,
                (byte) 0x72, (byte) 0x63, (byte) 0x68, (byte) 0x61, (byte) 0x72, (byte) 0xb8, (byte) 0x74, (byte) 0xe8,

                (byte) 0x80
        };
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }
}
