package com.ctrip.framework.drc.console.controller.monitor;

import com.ctrip.framework.drc.console.controller.AbstractControllerTest;
import com.ctrip.framework.drc.console.monitor.delay.config.ConsistencyMonitorConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.DelayMonitorConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.FullDataConsistencyCheckTestConfig;
import com.ctrip.framework.drc.console.service.monitor.ConsistencyMonitorService;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.framework.drc.console.vo.display.MhaGroupPair;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;

/**
 * Created by jixinwang on 2021/8/5
 */
public class ConsistencyMonitorControllerTest extends AbstractControllerTest {
    

    @InjectMocks
    private ConsistencyMonitorController controller;

    @Mock
    private ConsistencyMonitorService consistencyMonitorService;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        mvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    public void addDataConsistencyMonitor() throws Exception {
        DelayMonitorConfig delayMonitorConfig = new DelayMonitorConfig();
        MvcResult mvcResult = mvc.perform(post("/api/drc/v1/monitor/consistency/data/mhaA/mhaB")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtils.toJson(delayMonitorConfig))
                        .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        Assert.assertEquals(200, status);
    }

    @Test
    public void testAddFullDataConsistencyCheckForTest() throws Exception {
        FullDataConsistencyCheckTestConfig testConfig = new FullDataConsistencyCheckTestConfig();
        Mockito.doNothing().when(consistencyMonitorService).addFullDataConsistencyCheck(testConfig);
        MvcResult mvcResult = doNormalPost("/api/drc/v1/monitor/consistency/data/full/internalTest", testConfig);
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_SUCCESS);
    }

    @Test
    public void testFullDataCheckStatusForTest() throws Exception {
        Mockito.doReturn(null).when(consistencyMonitorService).getFullDataCheckStatusForTest("schema1", "table1", "key1");
        MvcResult mvcResult = doNormalGet("/api/drc/v1/monitor/consistency/data/full/checkStatusForTest/schema1/table1/key1");
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_SUCCESS);

        Mockito.doThrow(new SQLException()).when(consistencyMonitorService).getFullDataCheckStatusForTest("schema1", "table1", "key1");
        mvcResult = doNormalGet("/api/drc/v1/monitor/consistency/data/full/checkStatusForTest/schema1/table1/key1");
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_FAIL);
    }

    @Test
    public void testGetCurrentFullInconsistencyRecordForTest() throws Exception {
        FullDataConsistencyCheckTestConfig testConfig = new FullDataConsistencyCheckTestConfig();
        Mockito.doReturn(null).when(consistencyMonitorService).getCurrentFullInconsistencyRecordForTest(testConfig, "checkTime1");
        MvcResult mvcResult = doNormalPost("/api/drc/v1/monitor/consistency/full/historyForTest/checkTime1", testConfig);
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_SUCCESS);

        Mockito.doThrow(new SQLException()).when(consistencyMonitorService).getCurrentFullInconsistencyRecordForTest(testConfig, "checkTime1");
        mvcResult = doNormalPost("/api/drc/v1/monitor/consistency/full/historyForTest/checkTime1", testConfig);
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_FAIL);
    }

    @Test
    public void getDataConsistencyMonitor() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/api/drc/v1/monitor/consistency/data/mhaA/mhaB")
                        .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        Assert.assertEquals(200, status);
    }

    @Test
    public void deleteDataConsistencyMonitor() throws Exception {
        MvcResult mvcResult = mvc.perform(delete("/api/drc/v1/monitor/consistency/data/1")
                        .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        Assert.assertEquals(200, status);
    }

    @Test
    public void testSwitchDataConsistencyMonitor() throws Exception {
        Mockito.when(consistencyMonitorService.switchDataConsistencyMonitor(any(), any())).thenReturn(new HashMap<>());
        List<ConsistencyMonitorConfig> consistencyMonitorConfigs = Arrays.asList(new ConsistencyMonitorConfig(), new ConsistencyMonitorConfig());
        MvcResult mvcResult = mvc.perform(post("/api/drc/v1/monitor/consistency/switches/onOrOff")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtils.toJson(consistencyMonitorConfigs))
                        .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        Assert.assertEquals(200, status);
    }

    @Test
    public void testSwitchUnitVerification() throws Exception {
        Mockito.when(consistencyMonitorService.switchUnitVerification(any(), any())).thenReturn(true);
        MvcResult mvcResult = mvc.perform(post("/api/drc/v1/monitor/unit/switches/onOrOff")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtils.toJson(new MhaGroupPair()))
                        .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        Assert.assertEquals(200, status);
    }

}