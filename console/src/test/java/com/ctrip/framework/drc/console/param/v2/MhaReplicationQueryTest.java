package com.ctrip.framework.drc.console.param.v2;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class MhaReplicationQueryTest {
    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testEquals() throws Exception {
        MhaReplicationQuery query1 = new MhaReplicationQuery();
        MhaReplicationQuery query2 = new MhaReplicationQuery();
        assertEquals(query2, query1);
    }

    @Test
    public void testSet() throws Exception {
        MhaReplicationQuery query1 = new MhaReplicationQuery();


        query1.setDrcStatus(1);
        query1.setRelatedMhaIdList(Lists.newArrayList());
        query1.setDstMhaIdList(Lists.newArrayList());
        query1.setSrcMhaIdList(Lists.newArrayList());
        Integer drcStatus = query1.getDrcStatus();
        List<Long> relatedMhaIdList = query1.getRelatedMhaIdList();
        List<Long> dstMhaIdList = query1.getDstMhaIdList();
        List<Long> srcMhaIdList = query1.getSrcMhaIdList();
    }

}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme