package com.ctrip.framework.drc.core.driver.healthcheck.task;

import com.ctrip.framework.drc.core.AllTests;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class ExecutedGtidQueryTaskTest {
    public static Endpoint wrongCiEndpoint = new DefaultEndPoint(AllTests.IP,AllTests.SRC_PORT,AllTests.MYSQL_USER,AllTests.MYSQL_PASSWORD);

    @Test
    public void testDownGrade() {
        ExecutedGtidQueryTask executedGtidQueryTask = new ExecutedGtidQueryTask(wrongCiEndpoint);
        Assert.assertTrue(StringUtils.isEmpty(executedGtidQueryTask.doQuery()));
    }
}