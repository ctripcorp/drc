package com.ctrip.framework.drc.console.monitor.delay;


import com.ctrip.framework.drc.core.monitor.column.DelayInfo;
import com.ctrip.xpipe.api.codec.Codec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-09-03
 */
public class DelayMapTest{

    private DelayMap.DrcDirection drcDirection1;

    private DelayMap.DrcDirection drcDirection2;

    private static final String SRC_MHA = "shaoy";

    private static final String DEST_MHA = "sharb";

    private static final int DEFAULT_SIZE_OF_ExponentiallyDecayingReservoir = 1028;

    private static final double DELTA = 0.1;

    @Before
    public void setUp() {
        drcDirection1 = new DelayMap.DrcDirection(SRC_MHA, DEST_MHA);
        drcDirection2 = new DelayMap.DrcDirection(DEST_MHA, SRC_MHA);
    }

    @Test
    public void testDelayMap() {
        DelayMap delayMap = DelayMap.getInstance();
        delayMap.put(drcDirection1, 1);
        Assert.assertEquals(1, delayMap.avg(drcDirection1), DELTA);
        Assert.assertEquals(1, delayMap.size(drcDirection1));
        Assert.assertEquals(0, delayMap.avg(drcDirection2), DELTA);
        Assert.assertEquals(0, delayMap.size(drcDirection2));

        delayMap.put(drcDirection1, 3);
        Assert.assertEquals(2, delayMap.avg(drcDirection1), DELTA);
        Assert.assertEquals(2, delayMap.size(drcDirection1));

        delayMap.put(drcDirection2, 5);
        Assert.assertEquals(5, delayMap.avg(drcDirection2), DELTA);
        Assert.assertEquals(1, delayMap.size(drcDirection2));

        for(int i = 0; i < DEFAULT_SIZE_OF_ExponentiallyDecayingReservoir + 50; i++) {
            delayMap.put(drcDirection1, 3);
        }
        Assert.assertEquals(3, delayMap.avg(drcDirection1), DELTA);
        Assert.assertEquals(DEFAULT_SIZE_OF_ExponentiallyDecayingReservoir, delayMap.size(drcDirection1));


        delayMap.clear(drcDirection1);
        Assert.assertEquals(0, delayMap.avg(drcDirection1), 0.0);
        Assert.assertEquals(0, delayMap.size(drcDirection1));
    }
    
    @Test 
    public void testJson() {
        DelayInfo delayInfo = new DelayInfo("shaxy", "sha","mhaName");
        String UPSERT_SQL = "INSERT INTO `drcmonitordb`.`delaymonitor`(`id`, `src_ip`, `dest_ip`) VALUES(%s, '%s', '%s') ON DUPLICATE KEY UPDATE src_ip = '%s',dest_ip = '%s',datachange_lasttime = '%s';";        long timestampInMillis = System.currentTimeMillis();
        Timestamp timestamp = new Timestamp(timestampInMillis);
        String sql = String.format(UPSERT_SQL,3306, "shaxy",Codec.DEFAULT.encode(delayInfo),"shaxy",Codec.DEFAULT.encode(delayInfo),timestamp);
        System.out.println(sql);
    }
}