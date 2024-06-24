package com.ctrip.framework.drc.console.dto.v3;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author: yongnian
 * @create: 2024/6/24 11:27
 */
public class DbApplierSwitchReqDtoTest {
    @Test
    public void name() {

        DbApplierSwitchReqDto dbApplierSwitchReqDto = new DbApplierSwitchReqDto();
        dbApplierSwitchReqDto.setDbNames(Lists.newArrayList("db1","db2"));
        dbApplierSwitchReqDto.setSrcMhaName("srcMha");
        dbApplierSwitchReqDto.setDstMhaName("dstMha");
        Assert.assertEquals("srcMha",dbApplierSwitchReqDto.getSrcMhaName());
        Assert.assertEquals("dstMha",dbApplierSwitchReqDto.getDstMhaName());
    }
}

//Generated with love by TestMe :) Please raise issues & feature requests at: https://weirddev.com/forum#!/testme