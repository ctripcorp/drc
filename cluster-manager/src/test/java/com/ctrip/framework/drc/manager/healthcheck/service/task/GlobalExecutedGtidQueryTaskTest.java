package com.ctrip.framework.drc.manager.healthcheck.service.task;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import static com.ctrip.framework.drc.manager.AllTests.ciEndpoint;

public class GlobalExecutedGtidQueryTaskTest {

    @Test
    public void testGetExecutedGtid() {
        String executedGtid = new GlobalExecutedGtidQueryTask(ciEndpoint).doQuery();
        System.out.println(executedGtid);
        // for ci, gtid mode is not on
        Assert.assertTrue(StringUtils.isBlank(executedGtid));
    }
}
