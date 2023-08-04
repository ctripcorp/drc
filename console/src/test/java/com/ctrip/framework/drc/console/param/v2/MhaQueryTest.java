package com.ctrip.framework.drc.console.param.v2;

import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class MhaQueryTest {

    @Test
    public void emptyQueryCondition() {
        MhaQuery mhaQuery = new MhaQuery();
        Assert.assertTrue(mhaQuery.emptyQueryCondition());

        mhaQuery = new MhaQuery();
        mhaQuery.setDcIdList(Lists.newArrayList());
        Assert.assertTrue(mhaQuery.emptyQueryCondition());

        mhaQuery = new MhaQuery();
        mhaQuery.setContainMhaName("test");
        Assert.assertEquals(mhaQuery.getContainMhaName(), "test");
        Assert.assertFalse(mhaQuery.emptyQueryCondition());

        mhaQuery = new MhaQuery();
        mhaQuery.setBuId(1L);
        Assert.assertEquals((long) mhaQuery.getBuId(), 1L);
        Assert.assertFalse(mhaQuery.emptyQueryCondition());

        mhaQuery = new MhaQuery();
        List<Long> dcIdList = Lists.newArrayList(1L);
        mhaQuery.setDcIdList(dcIdList);
        Assert.assertEquals(mhaQuery.getDcIdList(), dcIdList);
        Assert.assertFalse(mhaQuery.emptyQueryCondition());
    }

}