package com.ctrip.framework.drc.core.config;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

/**
 * Created by jixinwang on 2022/8/10
 */
public class RegionConfigTest {

    @Test
    public void getRegion() {
        Assert.assertEquals("sin", RegionConfig.getInstance().getRegion());
    }

    @Test
    public void getRegion2dcsMapping() {
        Map<String, Set<String>> mappings = RegionConfig.getInstance().getRegion2dcsMapping();
        Assert.assertEquals(7, mappings.size());
    }

    @Test
    public void getDc2regionMap() {
        Map<String, String> mappings = RegionConfig.getInstance().getDc2regionMap();
        Assert.assertEquals(12, mappings.size());
    }

    @Test
    public void getCMRegionUrls() {
        Map<String, String> mappings = RegionConfig.getInstance().getCMRegionUrls();
        Assert.assertEquals(1, mappings.size());
    }

    @Test
    public void getConsoleRegionUrls() {
        Map<String, String> mappings = RegionConfig.getInstance().getConsoleRegionUrls();
        Assert.assertEquals(1, mappings.size());
    }
}
