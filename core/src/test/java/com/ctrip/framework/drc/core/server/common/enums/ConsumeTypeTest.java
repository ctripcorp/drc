package com.ctrip.framework.drc.core.server.common.enums;

import org.junit.Assert;
import org.junit.Test;

/**
 * @Author limingdong
 * @create 2022/10/9
 */
public class ConsumeTypeTest {

    @Test
    public void requestAllBinlog() {
        Assert.assertFalse(ConsumeType.Applier.requestAllBinlog());
        Assert.assertTrue(ConsumeType.Replicator.requestAllBinlog());
        Assert.assertFalse(ConsumeType.Console.requestAllBinlog());
        Assert.assertTrue(ConsumeType.Messenger.requestAllBinlog());
    }
}