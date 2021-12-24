package com.ctrip.framework.drc.validation.server;

import com.ctrip.framework.drc.core.server.config.validation.dto.ValidationConfigDto;
import com.ctrip.framework.drc.validation.AllTests;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author: Haibo Shen
 * @Date: 2021/3/27
 */
public class ValidationServerInClusterTest {

    @Test
    public void testEquals() throws Exception {
        ValidationConfigDto configDto1 = AllTests.getValidationConfigDto();
        ValidationConfigDto configDto2 = AllTests.getValidationConfigDto();
        ValidationServerInCluster server = new ValidationServerInCluster(configDto2);
        Assert.assertEquals(server.config, configDto1);
        Assert.assertNotSame(server.config, configDto1);
        configDto1.replicator.setIp("127.0.0.2");
        Assert.assertNotEquals(server.config, configDto1);
    }
}
