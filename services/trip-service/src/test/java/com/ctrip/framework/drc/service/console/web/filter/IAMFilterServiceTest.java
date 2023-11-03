package com.ctrip.framework.drc.service.console.web.filter;

import static org.junit.Assert.*;

import com.ctrip.framework.drc.service.config.QConfig;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class IAMFilterServiceTest {
    
    @InjectMocks
    private IAMFilterService iAMFilterService;
    
    @Mock
    private QConfig qConfig;
    
    @Before
    public void setUp() {
        System.setProperty("iam.config.enable","off");
        MockitoAnnotations.openMocks(this);
        Mockito.when(qConfig.get(Mockito.eq("iam.filter.switch"), Mockito.any())).thenReturn("on");
    }
    
    @Test
    public void testGetApiPermissionCode() {
        Mockito.when(qConfig.get(Mockito.eq("/api1"), Mockito.any())).thenReturn("code1");
        Mockito.when(qConfig.get(Mockito.eq("/api2"), Mockito.any())).thenReturn("code2");
        Mockito.when(qConfig.getKeys()).thenReturn(Sets.newHashSet("/api1"));
        iAMFilterService.onChange("/api1", null,null);
        Assert.assertEquals("code1", iAMFilterService.getApiPermissionCode("http://www.test.com/api1/1"));
        Assert.assertNull(iAMFilterService.getApiPermissionCode("http://www.test.com/api2/2"));
        Mockito.when(qConfig.getKeys()).thenReturn(Sets.newHashSet("/api1","/api2"));
        iAMFilterService.onChange("/api2", null,null);
        Assert.assertEquals("code2", iAMFilterService.getApiPermissionCode("http://www.test.com/api2/2"));
        
    }
    
    
    @Test
    public void testIamFilterEnable() {
        assertTrue(iAMFilterService.iamFilterEnable());
    }
}