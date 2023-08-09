package com.ctrip.framework.drc.core.http;

import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.util.CollectionUtils;

import java.util.List;

public class PageResultTest {

    @Test
    public void newInstance() {

        List<Long> data = Lists.newArrayList(1L, 2L);
        PageResult<Long> pageResult = PageResult.newInstance(data, 1, 10, 2);

        Assert.assertEquals(2, pageResult.getTotalCount());
        Assert.assertEquals(data, pageResult.getData());
    }

    @Test
    public void emptyResult() {
        PageResult<Long> emptyResult = PageResult.emptyResult();

        Assert.assertEquals(0, emptyResult.getTotalCount());
        Assert.assertTrue(CollectionUtils.isEmpty(emptyResult.getData()));
    }

}