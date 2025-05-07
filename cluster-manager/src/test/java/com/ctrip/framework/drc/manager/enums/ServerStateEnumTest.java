package com.ctrip.framework.drc.manager.enums;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author yongnian
 * @create 2024/11/4 15:39
 */
public class ServerStateEnumTest {
    @Test
    public void testPush() {
        Assert.assertFalse(ServerStateEnum.NORMAL.pushTo(ServerStateEnum.NORMAL));
        Assert.assertFalse(ServerStateEnum.NORMAL.pushTo(ServerStateEnum.RESTARTING));
        Assert.assertTrue(ServerStateEnum.NORMAL.pushTo(ServerStateEnum.LOST));

        Assert.assertTrue(ServerStateEnum.RESTARTING.pushTo(ServerStateEnum.NORMAL));
        Assert.assertFalse(ServerStateEnum.RESTARTING.pushTo(ServerStateEnum.RESTARTING));
        Assert.assertTrue(ServerStateEnum.RESTARTING.pushTo(ServerStateEnum.LOST));

        Assert.assertFalse(ServerStateEnum.LOST.pushTo(ServerStateEnum.NORMAL));
        Assert.assertTrue(ServerStateEnum.LOST.pushTo(ServerStateEnum.RESTARTING));
        Assert.assertFalse(ServerStateEnum.LOST.pushTo(ServerStateEnum.LOST));
    }
}