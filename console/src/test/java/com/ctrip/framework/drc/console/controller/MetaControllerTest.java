package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.controller.v1.MetaController;
import com.ctrip.framework.drc.console.dto.ProxyDto;
import com.ctrip.framework.drc.console.service.v2.resource.ProxyService;
import com.ctrip.framework.drc.console.service.v2.resource.RouteService;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
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

import java.util.Arrays;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-08-11
 */
public class MetaControllerTest extends AbstractControllerTest {

    @InjectMocks
    private MetaController controller;

    @Mock
    private RouteService routeService;

    @Mock
    private ProxyService proxyService;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        mvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    public void testInputDc() throws Exception {
        Mockito.doReturn(ApiResult.getSuccessInstance(true)).when(proxyService).inputDc(anyString());
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/meta/dcs/testDc").accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        assertNormalResponse(mvcResult);
    }

    @Test
    public void testInputBu() throws Exception {
        Mockito.doReturn(ApiResult.getSuccessInstance(true)).when(proxyService).inputBu(anyString());
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/meta/orgs/testOrg").accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        assertNormalResponse(mvcResult);
    }

    @Test
    public void testDeleteProxy() throws Exception {
        Mockito.doReturn(ApiResult.getSuccessInstance(true)).when(proxyService).deleteProxy(any());
        MvcResult mvcResult = doNormalDelete("/api/drc/v1/meta/proxy", new ProxyDto());
        assertNormalResponse(mvcResult);
    }

    @Test
    public void testGetProxyUris() throws Throwable {
        Mockito.doReturn(Arrays.asList("ip1", "ip2")).when(proxyService).getProxyUris("dc1");
        MvcResult mvcResult = doNormalGet("/api/drc/v1/meta/proxy/uris/dcs/dc1");
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_SUCCESS);

        Mockito.doThrow(new Exception()).when(proxyService).getProxyUris("dc1");
        mvcResult = doNormalGet("/api/drc/v1/meta/proxy/uris/dcs/dc1");
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_FAIL);
    }

    @Test
    public void testGetAllProxyUris() throws Throwable {
        Mockito.doReturn(Arrays.asList("ip1", "ip2")).when(proxyService).getRelayProxyUris();
        MvcResult mvcResult = doNormalGet("/api/drc/v1/meta/proxy/uris");
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_SUCCESS);

        Mockito.doThrow(new Exception()).when(proxyService).getRelayProxyUris();
        mvcResult = doNormalGet("/api/drc/v1/meta/proxy/uris");
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_FAIL);
    }

    
  
    @After
    public void tearDown() throws Exception {
    }
    

}