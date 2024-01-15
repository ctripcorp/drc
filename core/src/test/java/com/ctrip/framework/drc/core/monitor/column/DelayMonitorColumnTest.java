package com.ctrip.framework.drc.core.monitor.column;

import com.ctrip.framework.drc.core.driver.binlog.impl.DelayMonitorLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.UpdateRowsEvent;
import com.ctrip.xpipe.api.codec.Codec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * Created by jixinwang on 2021/11/11
 */
public class DelayMonitorColumnTest {

    private static final String gtid = "abcde123-5678-1234-abcd-abcd1234abcd:123456789";

    private static final String idc = "ntgxh";

    private DelayMonitorLogEvent delayMonitorLogEvent;

    @Before
    public void setUp() {
        ByteBuf eventByteBuf = initByteBuf();
        delayMonitorLogEvent = new DelayMonitorLogEvent(gtid, new UpdateRowsEvent().read(eventByteBuf));
        delayMonitorLogEvent.setSrcDcName(idc);
        eventByteBuf.release();
    }

    @Test
    public void getDelayMonitorSrcDcName() {
        String srcDcName = DelayMonitorColumn.getDelayMonitorSrcRegionName(delayMonitorLogEvent);
        Assert.assertEquals(idc, srcDcName);
        delayMonitorLogEvent.release();
    }

    @Test
    public void testRegion() {
        Assert.assertEquals(DelayMonitorColumn.transform(idc, ""), idc);
        String region = "sha";
        String mha = "mha";
        DelayInfo idcObject = new DelayInfo(idc, region, mha);
        Assert.assertEquals(DelayMonitorColumn.transform("", Codec.DEFAULT.encode(idcObject)), region);
    }

    /**
     * # at 4956664
     * #211111 15:32:21 server id 33213  end_log_pos 72575175 CRC32 0x133b9f93
     * # Position  Timestamp   Type   Master ID        Size      Master Pos    Flags
     * #   4ba1f8 05 c7 8c 61 1f bd 81 00  00 64 00 00 00 c7 68 53 04 00 00
     * #   4ba20b 8e 00 00 00 00 00 01 00  02 00 04 ff ff f0 fa 0c |................|
     * #   4ba21b 00 00 00 00 00 00 05 6e  74 67 78 68 09 00 64 72 |.......ntgxh..dr|
     * #   4ba22b 63 74 65 73 74 6e 74 61  8c c7 04 1d ce f0 fa 0c |ctestnta........|
     * #   4ba23b 00 00 00 00 00 00 05 6e  74 67 78 68 09 00 64 72 |.......ntgxh..dr|
     * #   4ba24b 63 74 65 73 74 6e 74 61  8c c7 05 1e a0 93 9f 3b |ctestnta........|
     * #   4ba25b 13                                               |.|
     * #       Update_rows: table id 142 flags: STMT_END_F
     */
    private ByteBuf initByteBuf() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(98);
        byte[] bytes = new byte[] {
                (byte) 0x05, (byte) 0xc7, (byte) 0x8c, (byte) 0x61, (byte) 0x1f, (byte) 0xbd, (byte) 0x81, (byte) 0x00, (byte) 0x00, (byte) 0x64, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xc7, (byte) 0x68, (byte) 0x53, (byte) 0x04, (byte) 0x00, (byte) 0x00,
                (byte) 0x8e, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x02, (byte) 0x00, (byte) 0x04, (byte) 0xff, (byte) 0xff, (byte) 0xf0, (byte) 0xfa, (byte) 0x0c,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x05, (byte) 0x6e, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x09, (byte) 0x00, (byte) 0x64, (byte) 0x72,
                (byte) 0x63, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x6e, (byte) 0x74, (byte) 0x61, (byte) 0x8c, (byte) 0xc7, (byte) 0x04, (byte) 0x1d, (byte) 0xce, (byte) 0xf0, (byte) 0xfa, (byte) 0x0c,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x05, (byte) 0x6e, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x09, (byte) 0x00, (byte) 0x64, (byte) 0x72,
                (byte) 0x63, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x6e, (byte) 0x74, (byte) 0x61, (byte) 0x8c, (byte) 0xc7, (byte) 0x05, (byte) 0x1e, (byte) 0xa0, (byte) 0x93, (byte) 0x9f, (byte) 0x3b,
                (byte) 0x13
        };
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }
}
