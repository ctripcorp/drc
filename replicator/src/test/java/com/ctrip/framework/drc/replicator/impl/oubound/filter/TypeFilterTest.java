package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
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

        OutboundLogEventContext context = new OutboundLogEventContext();

        TypeFilter typeFilter = new TypeFilter(ConsumeType.Replicator);
        Assert.assertFalse(typeFilter.doFilter(context));

        typeFilter = new TypeFilter(ConsumeType.Applier);
        context.reset(0);
        context.setEventType(LogEventType.gtid_log_event);
        Assert.assertFalse(typeFilter.doFilter(context));

        context.setEventType(LogEventType.rows_query_log_event);
        context.reset(0);
        Assert.assertTrue(typeFilter.doFilter(context));

        typeFilter = new TypeFilter(ConsumeType.Messenger);
        context.reset(0);
        context.setEventType(LogEventType.gtid_log_event);
        Assert.assertFalse(typeFilter.doFilter(context));

        context.setEventType(LogEventType.rows_query_log_event);
        context.reset(0);
        Assert.assertTrue(typeFilter.doFilter(context));
    }
}