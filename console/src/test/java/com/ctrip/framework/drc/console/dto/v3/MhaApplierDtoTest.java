package com.ctrip.framework.drc.console.dto.v3;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author: yongnian
 * @create: 2024/6/24 11:24
 */
public class MhaApplierDtoTest {

    @Test
    public void name() {
        MhaApplierDto mhaApplierDto = new MhaApplierDto();
        mhaApplierDto.setIps(Lists.newArrayList("11.22.33.44","44.33.22.11"));
        mhaApplierDto.setGtidInit("abcd:1-1000");
        Assert.assertEquals(Lists.newArrayList("11.22.33.44", "44.33.22.11"), mhaApplierDto.getIps());
        Assert.assertEquals("abcd:1-1000",mhaApplierDto.getGtidInit());


        mhaApplierDto = new MhaApplierDto(Lists.newArrayList("11.22.33.44"),"abc:1-1000");
        Assert.assertEquals(Lists.newArrayList("11.22.33.44"), mhaApplierDto.getIps());
        Assert.assertEquals("abc:1-1000",mhaApplierDto.getGtidInit());

    }
}

//Generated with love by TestMe :) Please raise issues & feature requests at: https://weirddev.com/forum#!/testme