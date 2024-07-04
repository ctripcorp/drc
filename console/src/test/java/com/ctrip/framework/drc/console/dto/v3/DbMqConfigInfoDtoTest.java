package com.ctrip.framework.drc.console.dto.v3;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static org.mockito.Mockito.*;

/**
 * @author: yongnian
 * @create: 2024/7/3 16:41
 */
public class DbMqConfigInfoDtoTest {
    @Test
    public void testGetter() {
        DbMqConfigInfoDto infoDto = new DbMqConfigInfoDto("srcRegion");
        infoDto.setDbNames(Lists.newArrayList("db1", "db2"));
        infoDto.setLogicTableSummaryDtos(Lists.newArrayList());
        infoDto.setMhaMqDtos(Lists.newArrayList());


        Assert.assertEquals(infoDto.getMhaMqDtos(), Lists.newArrayList());
        Assert.assertEquals(infoDto.getLogicTableSummaryDtos(), Lists.newArrayList());
        Assert.assertEquals(infoDto.getDbNames(), Lists.newArrayList("db1", "db2"));
        Assert.assertEquals(infoDto.getSrcRegionName(), "srcRegion");

        DbMqConfigInfoDto infoDto2 = new DbMqConfigInfoDto("srcRegion");
        infoDto2.setDbNames(Lists.newArrayList("db1", "db2"));
        infoDto2.setLogicTableSummaryDtos(Lists.newArrayList());
        infoDto2.setMhaMqDtos(Lists.newArrayList());

        Assert.assertEquals(infoDto2,infoDto);
    }
}

//Generated with love by TestMe :) Please raise issues & feature requests at: https://weirddev.com/forum#!/testme