package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.impl.ConfigServiceImpl;
import com.ctrip.framework.drc.console.service.impl.DrcMaintenanceServiceImpl;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.mockito.Mockito.when;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-01-07
 */
public class ConfigControllerTest {

    private MockMvc mvc;

    @InjectMocks
    private ConfigController controller;

    @Mock
    private ConfigServiceImpl configService;

    @Mock
    private DrcMaintenanceServiceImpl drcMaintenanceService;

    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;

    private ObjectMapper objectMapper = new ObjectMapper();

    @Before
    public void setUp() {
        String[] dataTypes = {"int", "char", "boolean"};

        /** initialization */
        MockitoAnnotations.initMocks(this);
        /** build mvc env */
        mvc = MockMvcBuilders.standaloneSetup(controller).build();

        when(configService.getAllDrcSupportDataTypes()).thenReturn(dataTypes);
        when(drcMaintenanceService.changeMasterDb(Mockito.anyString(), Mockito.anyString(), Mockito.anyInt())).thenReturn(ApiResult.getSuccessInstance(""));
    }

    @Test
    public void testNewNotifyMasterDb() throws Exception {
        DefaultEndPoint endpoint = new DefaultEndPoint("127.0.0.1", 3306);
        String jsonStr = objectMapper.writeValueAsString(endpoint);
        System.out.println("requestStr: " +  jsonStr);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/mhas/testMhaName/instances").contentType(MediaType.APPLICATION_JSON).content(jsonStr))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        MockHttpServletResponse response = mvcResult.getResponse();
        int status = response.getStatus();
        Assert.assertEquals(200, status);
        String responseStr = response.getContentAsString();
        System.out.println("responseStr: " + responseStr);
        JsonNode result = objectMapper.readTree(responseStr);
        Assert.assertEquals(ResultCode.HANDLE_SUCCESS.getCode(), result.get("status").asInt());
        Assert.assertEquals(ResultCode.HANDLE_SUCCESS.getMessage(), result.get("message").asText());
    }

    @Test
    public void testGetAllDrcSupportDataType() throws Exception {

        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/data/types").accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        Assert.assertNotNull(response);
        Assert.assertNotEquals("", response);
    }

    @Test
    public void testGetHealthStatus() throws Exception {
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/health").accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
        Assert.assertNotNull(response);
        Assert.assertNotEquals("", response);
    }

    @Test
    public void testGetUnitResultHickwallAddress() {
        Mockito.doReturn("").when(monitorTableSourceProvider).getUnitResultHickwallAddress();
        ApiResult unitResultHickwallAddress = controller.getUnitResultHickwallAddress();
        Assert.assertEquals(ResultCode.HANDLE_SUCCESS.getCode(), unitResultHickwallAddress.getStatus().intValue());
    }
}
