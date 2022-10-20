package com.ctrip.framework.drc.console.config;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class UdlMigrateConfigurationTest {
    
    private UdlMigrateConfiguration config;
    
    @Before
    public void setUp() throws Exception {
        config = new UdlMigrateConfiguration();
    }

    @Test
    public void testGray() {
        Assert.assertTrue(config.gray(1L));
        Assert.assertFalse(config.gray(2L));
    }
}