package com.ctrip.framework.drc.core.service.ops;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class AppNodeTest {

    @Test
    public void testIsLegal() {
        AppNode appNode0 = new AppNode();
        appNode0.setIp("");
        appNode0.setPort(8080);

        AppNode appNode1 = new AppNode();
        appNode1.setIp("ip2");
        appNode1.setPort(8080);

        Assert.assertFalse(appNode0.isLegal());
        Assert.assertFalse(appNode0.equals(appNode1));
    }
}