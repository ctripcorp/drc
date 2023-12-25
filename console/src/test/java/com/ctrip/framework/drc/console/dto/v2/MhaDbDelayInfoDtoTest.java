package com.ctrip.framework.drc.console.dto.v2;

import org.junit.Assert;
import org.junit.Test;

public class MhaDbDelayInfoDtoTest {

    @Test
    public void testToString() throws Exception {
        String result = get().toString();
        System.out.println(result);
    }

    @Test
    public void testGetDelay() throws Exception {
        Long result = get().getDelay();
        Assert.assertEquals(Long.valueOf(2), result);
    }
    public MhaDbDelayInfoDto get(){
        MhaDbDelayInfoDto mhaDbDelayInfoDto = new MhaDbDelayInfoDto();
        mhaDbDelayInfoDto.setDbName("db1");
        mhaDbDelayInfoDto.setSrcMha("src");
        mhaDbDelayInfoDto.setDstMha("dst");
        mhaDbDelayInfoDto.setSrcTime(125L);
        mhaDbDelayInfoDto.setDstTime(123L);
        return mhaDbDelayInfoDto;
    }
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme