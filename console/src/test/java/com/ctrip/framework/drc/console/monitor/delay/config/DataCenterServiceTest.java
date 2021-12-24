package com.ctrip.framework.drc.console.monitor.delay.config;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-23
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:applicationContext.xml"})
public class DataCenterServiceTest {

    @Autowired
    private DataCenterService dataCenterService;

    @Test
    public void testService() {
        Assert.assertEquals("ntgxh", dataCenterService.getDc());
    }
}
