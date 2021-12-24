package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.dto.FailoverDto;
import com.ctrip.framework.drc.core.service.beacon.BeaconResult;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.impl.HealthServiceImpl;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.ctrip.framework.drc.core.service.beacon.BeaconResult.BEACON_FAILURE;
import static com.ctrip.framework.drc.core.service.beacon.BeaconResult.BEACON_SUCCESS;
import static org.mockito.Mockito.when;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-09-25
 */
public class HealthControllerTest {
    private MockMvc mvc;

    private ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    private HealthController controller;

    @Mock
    private HealthServiceImpl healthService;

    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        mvc = MockMvcBuilders.standaloneSetup(controller).build();

    }

    @Test
    public void testQueryClusterHealth() throws Exception {
        when(healthService.isDelayNormal("cluster1", "consoleGroup1")).thenReturn(true);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/beacon/health?clusterName=cluster1&groupName=consoleGroup1"))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        MockHttpServletResponse response = mvcResult.getResponse();
        int status = response.getStatus();
        String responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        JsonNode jsonNode = objectMapper.readTree(responseStr);
        Assert.assertEquals(0, jsonNode.get("code").asInt());
        Assert.assertEquals(BEACON_SUCCESS, jsonNode.get("msg").asText());
        Assert.assertTrue(jsonNode.get("data").get("result").asBoolean());

        when(healthService.isDelayNormal("cluster1", "consoleGroup1")).thenReturn(false);
        mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/beacon/health?clusterName=cluster1&groupName=consoleGroup1"))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        response = mvcResult.getResponse();
        status = response.getStatus();
        responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        jsonNode = objectMapper.readTree(responseStr);
        Assert.assertEquals(0, jsonNode.get("code").asInt());
        Assert.assertEquals(BEACON_FAILURE, jsonNode.get("msg").asText());
        Assert.assertFalse(jsonNode.get("data").get("result").asBoolean());
    }

    @Test
    public void testDoFailover() throws Exception {
        FailoverDto failoverDto = new FailoverDto();
        failoverDto.setClusterName("cluster1");
        failoverDto.setFailoverGroups(Arrays.asList("127.0.0.1:8080"));
        Map<String, String> extra = new HashMap<>();
        extra.put(HealthController.BEACON_FAILOVER_TOKEN_KEY, HealthController.BEACON_FAILOVER_TOKEN_VALUE);
        failoverDto.setExtra(extra);
        when(healthService.doFailover(Mockito.any())).thenReturn(BeaconResult.getSuccessInstance(true));
        when(monitorTableSourceProvider.getAllowFailoverSwitch()).thenReturn("on");

        String jsonStr = objectMapper.writeValueAsString(failoverDto);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/beacon/failover").contentType(MediaType.APPLICATION_JSON).content(jsonStr))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        MockHttpServletResponse response = mvcResult.getResponse();
        int status = response.getStatus();
        String responseStr = response.getContentAsString();
        System.out.println(responseStr);
        Assert.assertEquals(200, status);
        JsonNode jsonNode = objectMapper.readTree(responseStr);
        Assert.assertEquals(0, jsonNode.get("code").asInt());
        Assert.assertEquals(BEACON_SUCCESS, jsonNode.get("msg").asText());
        Assert.assertTrue(jsonNode.get("data").asBoolean());

        when(monitorTableSourceProvider.getAllowFailoverSwitch()).thenReturn("off");
        jsonStr = objectMapper.writeValueAsString(failoverDto);
        mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/beacon/failover").contentType(MediaType.APPLICATION_JSON).content(jsonStr))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        response = mvcResult.getResponse();
        status = response.getStatus();
        responseStr = response.getContentAsString();
        System.out.println(responseStr);
        Assert.assertEquals(200, status);
        jsonNode = objectMapper.readTree(responseStr);
        Assert.assertEquals(0, jsonNode.get("code").asInt());
        Assert.assertEquals(BEACON_SUCCESS, jsonNode.get("msg").asText());
        Assert.assertTrue(jsonNode.get("data").asBoolean());

        when(monitorTableSourceProvider.getAllowFailoverSwitch()).thenReturn("on");
        when(healthService.doFailover(Mockito.any())).thenReturn(BeaconResult.getFailInstanceWithoutRetry(false));
        mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/beacon/failover").contentType(MediaType.APPLICATION_JSON).content(jsonStr))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        response = mvcResult.getResponse();
        status = response.getStatus();
        responseStr = response.getContentAsString();
        System.out.println(responseStr);
        Assert.assertEquals(200, status);
        jsonNode = objectMapper.readTree(responseStr);
        Assert.assertEquals(-1, jsonNode.get("code").asInt());
        Assert.assertEquals(BEACON_FAILURE, jsonNode.get("msg").asText());
        Assert.assertFalse(jsonNode.get("data").asBoolean());
    }

    @Test
    public void testQueryUpdateFunction() throws Exception {
        when(healthService.isLocalDcUpdating("cluster1")).thenReturn(true);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/beacon/function/mhas/cluster1"))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        MockHttpServletResponse response = mvcResult.getResponse();
        int status = response.getStatus();
        String responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        BeaconResult result = objectMapper.readValue(responseStr, BeaconResult.class);
        Assert.assertEquals(0, result.getCode().intValue());
        Assert.assertEquals(BEACON_SUCCESS, result.getMsg());
        Assert.assertTrue((Boolean) result.getData());

        when(healthService.isLocalDcUpdating("cluster1")).thenReturn(false);
        mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/beacon/function/mhas/cluster1"))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        response = mvcResult.getResponse();
        status = response.getStatus();
        responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        result = objectMapper.readValue(responseStr, BeaconResult.class);
        Assert.assertEquals(1, result.getCode().intValue());
        Assert.assertEquals(BEACON_FAILURE, result.getMsg());
        Assert.assertFalse((Boolean) result.getData());
    }

    @After
    public void tearDown() {

    }

}
