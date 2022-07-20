package com.ctrip.framework.drc.console.monitor.delay.config;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class CompositeConfigTest {
    
    @InjectMocks
    CompositeConfig compositeConfig;
    
    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;
    
    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        Mockito.when(monitorTableSourceProvider.getDrcMetaXmlUpdateSwitch()).thenReturn("off");
    }

    @Test
    public void testUpdateConfig() {
        compositeConfig.initConfigs();
        compositeConfig.updateConfig();
        Assert.assertNull(compositeConfig.xml);

        compositeConfig.addConfig(new FileConfig());
        compositeConfig.updateConfig();
        Assert.assertNotNull(compositeConfig.xml);
    }
}
