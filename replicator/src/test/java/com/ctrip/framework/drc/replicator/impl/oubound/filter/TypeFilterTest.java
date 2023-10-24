package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import org.junit.Assert;
import org.junit.Test;

/**
 * @Author limingdong
 * @create 2022/4/23
 */
public class TypeFilterTest {

    @Test
    public void doFilter() {

        TypeFilter typeFilter = new TypeFilter(ConsumeType.Console);
        Assert.assertTrue(typeFilter.doFilter(new OutboundLogEventContext()));

        typeFilter = new TypeFilter(ConsumeType.Applier);
        Assert.assertTrue(typeFilter.doFilter(new OutboundLogEventContext()));

        typeFilter = new TypeFilter(ConsumeType.Applier);
        Assert.assertFalse(typeFilter.doFilter(new OutboundLogEventContext()));
    }
}