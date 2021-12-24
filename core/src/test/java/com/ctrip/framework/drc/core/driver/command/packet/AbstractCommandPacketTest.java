package com.ctrip.framework.drc.core.driver.command.packet;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.After;

/**
 * Created by mingdongli
 * 2019/10/8 上午9:10.
 */
public abstract class AbstractCommandPacketTest {

    protected ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.directBuffer();

    protected GtidSet gtidSet;

    protected static final String GTID_SET = "560f4cad-8c39-11e9-b53b-6c92bf463216:1-9223372036854775807";

    @After
    public void tearDown() {
        byteBuf.release();
    }

}
