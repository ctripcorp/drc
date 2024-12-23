package com.ctrip.framework.drc.console.dto.v3;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author: yongnian
 * @create: 2024/6/24 11:24
 */
public class MhaMessengerDtoTest {

    @Test
    public void name() {
        MhaMessengerDto mhaMessengerDto = new MhaMessengerDto();
        mhaMessengerDto.setIps(Lists.newArrayList("11.22.33.44","44.33.22.11"));
        mhaMessengerDto.setGtidInit("abcd:1-1000");
        Assert.assertEquals(Lists.newArrayList("11.22.33.44", "44.33.22.11"), mhaMessengerDto.getIps());
        Assert.assertEquals("abcd:1-1000", mhaMessengerDto.getGtidInit());


        mhaMessengerDto = new MhaMessengerDto(Lists.newArrayList("11.22.33.44"),"abc:1-1000");
        Assert.assertEquals(Lists.newArrayList("11.22.33.44"), mhaMessengerDto.getIps());
        Assert.assertEquals("abc:1-1000", mhaMessengerDto.getGtidInit());

    }
}

//Generated with love by TestMe :) Please raise issues & feature requests at: https://weirddev.com/forum#!/testme