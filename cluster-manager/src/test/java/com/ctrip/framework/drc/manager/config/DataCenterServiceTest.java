package com.ctrip.framework.drc.manager.config;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Set;


/**
 * @Author limingdong
 * @create 2022/6/21
 */
public class DataCenterServiceTest {

    private DataCenterService dataCenterService = new DataCenterService();

    @Before
    public void setUp(){
        dataCenterService.afterPropertiesSet();
    }

    @Test
    public void getDc() {
        Assert.assertEquals("ntgxh", dataCenterService.getDc());
    }

    @Test
    public void getRegion() {
        Assert.assertEquals("sin", dataCenterService.getRegion());
    }

    @Test
    public void getRegionByDcName() {
        Assert.assertEquals("sin", dataCenterService.getRegion("SINAWS"));
    }

    @Test
    public void getRegionIdcMapping() {
        Map<String, Set<String>> mappings = dataCenterService.getRegionIdcMapping();
        Assert.assertEquals(7, mappings.size());
    }
}
