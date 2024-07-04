package com.ctrip.framework.drc.console.service.v2.security.impl;

import static org.junit.Assert.*;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.param.v2.security.Account;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.EnvUtils;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.service.security.HeraldService;
import com.ctrip.framework.foundation.Foundation;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Maps;
import java.util.Map;
import javax.servlet.annotation.HandlesTypes;
import org.apache.commons.lang3.StringUtils;
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