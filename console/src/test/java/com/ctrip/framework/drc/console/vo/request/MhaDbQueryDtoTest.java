package com.ctrip.framework.drc.console.vo.request;

import org.junit.Assert;
import org.junit.Test;

public class MhaDbQueryDtoTest {

    @Test
    public void testIsConditionalQuery() throws Exception {
        MhaDbQueryDto mhaDbQueryDto = new MhaDbQueryDto();
        Assert.assertFalse(mhaDbQueryDto.isConditionalQuery());
        mhaDbQueryDto.setRegionId(1L);
        Assert.assertTrue(mhaDbQueryDto.isConditionalQuery());
    }

    @Test
    public void testHasDbCondition() throws Exception {
        MhaDbQueryDto mhaDbQueryDto = new MhaDbQueryDto();
        Assert.assertFalse(mhaDbQueryDto.hasDbCondition());
        mhaDbQueryDto.setBuCode("some");
        Assert.assertTrue(mhaDbQueryDto.hasDbCondition());
    }

    @Test
    public void testHasMhaCondition() throws Exception {
        MhaDbQueryDto mhaDbQueryDto = new MhaDbQueryDto();
        Assert.assertFalse(mhaDbQueryDto.hasMhaCondition());
        mhaDbQueryDto.setRegionId(1L);
        Assert.assertTrue(mhaDbQueryDto.hasMhaCondition());
    }
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme