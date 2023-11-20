package com.ctrip.framework.drc.console.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import com.ctrip.framework.drc.console.controller.v1.MetaController;
import com.ctrip.framework.drc.console.dto.ProxyDto;
import com.ctrip.framework.drc.console.service.impl.DrcMaintenanceServiceImpl;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceTwoImpl;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
import java.util.Arrays;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-08-11
 */
public class MetaControllerTest extends AbstractControllerTest {

    @InjectMocks
    private MetaController controller;

    
    @Mock
    private MetaInfoServiceImpl metaInfoService;

    @Mock
    private DrcMaintenanceServiceImpl drcMaintenanceService;
    
    @Mock
    private MetaInfoServiceTwoImpl metaInfoServiceTwo;
    
    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        mvc = MockMvcBuilders.standaloneSetup(controller).build();
    }
    

    @Test
    public void testDeleteProxyRoute() throws Exception {
        Mockito.doReturn(ApiResult.getSuccessInstance(true)).when(drcMaintenanceService).deleteRoute(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),Mockito.anyString());
        MvcResult mvcResult = doNormalDelete("/api/drc/v1/meta/routes/proxy?routeOrgName=testBu&srcDcName=testSrc&dstDcName=testDst&tag=meta");
        assertNormalResponse(mvcResult);
    }

    @Test
    public void testGetProxyRoutes() throws Exception {
        Mockito.doReturn(Lists.newArrayList()).when(metaInfoService).getRoutes(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),Mockito.anyString(),Mockito.any());
        MvcResult mvcResult = doNormalGet("/api/drc/v1/meta/routes?routeOrgName=testBu&srcDcName=testSrc&dstDcName=testDst&tag=meta&deleted=0");
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_SUCCESS);
    }

    @Test
    public void testInputDc() throws Exception {
        Mockito.doReturn(ApiResult.getSuccessInstance(true)).when(drcMaintenanceService).inputDc(anyString());
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/meta/dcs/testDc").accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        assertNormalResponse(mvcResult);
    }

    @Test
    public void testInputBu() throws Exception {
        Mockito.doReturn(ApiResult.getSuccessInstance(true)).when(drcMaintenanceService).inputBu(anyString());
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/meta/orgs/testOrg").accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        assertNormalResponse(mvcResult);
    }

    @Test
    public void testInputProxy() throws Exception {
        Mockito.doReturn(ApiResult.getSuccessInstance(true)).when(drcMaintenanceService).inputProxy(any());
        MvcResult mvcResult = doNormalPost("/api/drc/v1/meta/proxy", new ProxyDto());
        assertNormalResponse(mvcResult);
    }

    @Test
    public void testDeleteProxy() throws Exception {
        Mockito.doReturn(ApiResult.getSuccessInstance(true)).when(drcMaintenanceService).deleteProxy(any());
        MvcResult mvcResult = doNormalDelete("/api/drc/v1/meta/proxy", new ProxyDto());
        assertNormalResponse(mvcResult);
    }

    @Test
    public void testGetProxyUris() throws Throwable {
        Mockito.doReturn(Arrays.asList("ip1", "ip2")).when(metaInfoServiceTwo).getProxyUris("dc1");
        MvcResult mvcResult = doNormalGet("/api/drc/v1/meta/proxy/uris/dcs/dc1");
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_SUCCESS);

        Mockito.doThrow(new Throwable()).when(metaInfoServiceTwo).getProxyUris("dc1");
        mvcResult = doNormalGet("/api/drc/v1/meta/proxy/uris/dcs/dc1");
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_FAIL);
    }

    @Test
    public void testGetAllProxyUris() throws Throwable {
        Mockito.doReturn(Arrays.asList("ip1", "ip2")).when(metaInfoServiceTwo).getAllProxyUris();
        MvcResult mvcResult = doNormalGet("/api/drc/v1/meta/proxy/uris");
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_SUCCESS);

        Mockito.doThrow(new Throwable()).when(metaInfoServiceTwo).getAllProxyUris();
        mvcResult = doNormalGet("/api/drc/v1/meta/proxy/uris");
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_FAIL);
    }

    
  
    @After
    public void tearDown() throws Exception {
    }
    

}