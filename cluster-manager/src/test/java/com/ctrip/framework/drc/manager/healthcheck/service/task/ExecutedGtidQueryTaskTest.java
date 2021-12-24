package com.ctrip.framework.drc.manager.healthcheck.service.task;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import static com.ctrip.framework.drc.manager.AllTests.*;

public class ExecutedGtidQueryTaskTest {

    public static Endpoint wrongCiEndpoint = new DefaultEndPoint(CI_MYSQL_IP, CI_PORT1, CI_MYSQL_USER, CI_MYSQL_USER);

    @Test
    public void testDownGrade() {
        ExecutedGtidQueryTask executedGtidQueryTask = new ExecutedGtidQueryTask(wrongCiEndpoint);
        Assert.assertTrue(StringUtils.isEmpty(executedGtidQueryTask.doQuery()));
    }
}
