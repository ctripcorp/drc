package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.service.SSOService;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.service.ops.AppClusterResult;
import com.ctrip.framework.drc.core.service.ops.AppNode;
import com.ctrip.framework.drc.core.service.ops.OPSApiService;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.net.UnknownHostException;
import java.util.ArrayList;


public class SSOServiceImplTest {

    @InjectMocks
    SSOServiceImpl ssoService;
    
    @Spy
    DomainConfig domainConfig = new DomainConfig();
    
    @Mock
    OPSApiService opsApiService;
    
    private final String default_url = "http://localhost:8080/ops/getFATServers";
    private final String degradeUrl = "http://ip1:8080/api/drc/v1/access/sso/degrade/notify/true";
    
    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }
    
    @Test
    public void testNotifyOtherMachine()  throws UnknownHostException  {
        AppNode appNode = new AppNode();
        appNode.setIp("ip1");
        appNode.setPort(8080);
        AppNode appNode1 = new AppNode();
        appNode1.setIp("localhost");
        appNode1.setPort(8080);
        ArrayList<AppNode> appNodes = Lists.newArrayList(appNode, appNode1);
        Mockito.doReturn(appNodes).when(opsApiService).
                getAppNodes(Mockito.eq(default_url),Mockito.any(),Mockito.anyList(),Mockito.eq("FAT"));
        
        try (MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when(()-> HttpUtils.post(Mockito.eq(degradeUrl),Mockito.any(),Mockito.any())).thenReturn(ApiResult.getSuccessInstance("set ssoDegradeSwitch to : true"));
            ApiResult apiResult = ssoService.notifyOtherMachine(true);
            Assert.assertTrue(apiResult.getStatus().equals(ResultCode.HANDLE_SUCCESS.getCode()));
        }

        try (MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when(()-> HttpUtils.post(Mockito.eq(degradeUrl),Mockito.any(),Mockito.any())).thenReturn(ApiResult.getFailInstance("changeSSOFilterStatus occur error"));
            ApiResult apiResult = ssoService.notifyOtherMachine(true);
            Assert.assertTrue(apiResult.getStatus().equals(ResultCode.HANDLE_SUCCESS.getCode()));
        }
    }
    
   
}