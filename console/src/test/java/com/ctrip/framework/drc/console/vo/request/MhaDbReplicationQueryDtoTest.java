package com.ctrip.framework.drc.console.vo.request;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

public class MhaDbReplicationQueryDtoTest {
    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testSetGet() throws Exception {
        MhaDbReplicationQueryDto dto = new MhaDbReplicationQueryDto();
        MhaDbQueryDto srcMhaDb = new MhaDbQueryDto();
        dto.setSrcMhaDb(srcMhaDb);
        Assert.assertEquals(srcMhaDb, dto.getSrcMhaDb());
        dto.setDstMhaDb(srcMhaDb);
        Assert.assertEquals(srcMhaDb, dto.getDstMhaDb());
        dto.setRelatedMhaDb(srcMhaDb);
        Assert.assertEquals(srcMhaDb, dto.getRelatedMhaDb());

        dto.setDrcStatus(1);
        Assert.assertEquals(1, (int) dto.getDrcStatus());
        dto.setTotalCount(33);
        Assert.assertEquals(33, dto.getTotalCount());

        dto.setPageIndex(2);
        Assert.assertEquals(2, dto.getPageIndex());
        dto.setPageSize(30);
        Assert.assertEquals(30, dto.getPageSize());

    }
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme