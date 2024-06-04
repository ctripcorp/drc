package com.ctrip.framework.drc.console.service.v2.security.impl;

import static org.junit.Assert.*;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.service.security.HeraldService;
import com.ctrip.framework.foundation.Foundation;
import com.google.common.collect.Maps;
import java.util.Map;
import javax.servlet.annotation.HandlesTypes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class KmsServiceImplTest {
    
    @InjectMocks
    KmsServiceImpl kmsServiceImpl;
    
    @Mock
    HeraldService heraldService;
    
    @Mock
    DefaultConsoleConfig consoleConfig;


    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
    }
    
    @Test
    public void testGetSecretKey() {
    }

    
}