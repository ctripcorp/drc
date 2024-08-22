package com.ctrip.framework.drc.core.monitor.column;

import org.junit.Assert;
import org.junit.Test;

public class DbDelayDtoTest {

    @Test
    public void testParseDelayInfo() {
        String dc = "ntgxh";
        String region = "ntgxh";
        String mha = "zyn_test_1";
        String db = "zyn_test_shard_db498";
        DbDelayDto.DelayInfo from = DbDelayDto.DelayInfo.from(dc, region, mha, db);

        String json = "{\"d\":\"ntgxh\",\"r\":\"ntgxh\",\"m\":\"zyn_test_1\",\"b\":\"zyn_test_shard_db498\"}";
        Assert.assertEquals(json,from.toJson());
        Assert.assertEquals(json,from.toJson());
        Assert.assertEquals(json,from.toJson());

        DbDelayDto.DelayInfo parsed = DbDelayDto.DelayInfo.parse(json);
        Assert.assertEquals(from,parsed);
    }

    @Test
    public void testParseDelayDto() {
        String dc = "ntgxh";
        String region = "ntgxh";
        String mha = "zyn_test_1";
        String db = "zyn_test_shard_db498";
        DbDelayDto.DelayInfo from = DbDelayDto.DelayInfo.from(dc, region, mha, db);


        Long datachangeLasttime = 1111111L;
        DbDelayDto dto = DbDelayDto.from(11L, from, datachangeLasttime);
        Assert.assertEquals(dc,dto.getDcName());
        Assert.assertEquals(region,dto.getRegion());
        Assert.assertEquals(mha,dto.getMha());
        Assert.assertEquals(db,dto.getDbName());
        Assert.assertEquals(11L,dto.getId());
        Assert.assertEquals(datachangeLasttime,dto.getDatachangeLasttime());
        String string = dto.toString();
    }

}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme