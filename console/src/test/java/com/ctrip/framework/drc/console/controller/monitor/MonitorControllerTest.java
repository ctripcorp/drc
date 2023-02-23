package com.ctrip.framework.drc.console.controller.monitor;

import com.ctrip.framework.drc.console.controller.AbstractControllerTest;
import com.ctrip.framework.drc.console.dao.MachineTblDao;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.service.monitor.MonitorService;
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
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.sql.SQLException;

import static org.mockito.ArgumentMatchers.any;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;

/**
 * Created by jixinwang on 2020/12/30
 */
public class MonitorControllerTest extends AbstractControllerTest {

    private MockMvc mvc;

    @InjectMocks
    private MonitorController controller;

    @Mock
    private MonitorService monitorService;
    
    @Mock
    private MachineTblDao machineTblDao;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        mvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    public void testSwitchMonitors() throws Exception {
        MvcResult mvcResult = mvc.perform(post("/api/drc/v1/monitor/switches/on")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtils.toJson(new MhaGroupPair()))
                .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        Assert.assertEquals(200, status);
    }

    @Test
    public void testGetMhaNamesToBeMonitored() throws Exception {
        MvcResult mvcResult = mvc.perform(post("/api/drc/v1/monitor/switches/on")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtils.toJson(new MhaGroupPair()))
                .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        Assert.assertEquals(200, status);
    }

    @Test
    public void testgetUuidFromDBForRemoteDC() throws Exception {
        MachineTbl machineTbl = new MachineTbl();
        machineTbl.setUuid("uuid1");
        Mockito.doReturn(machineTbl).when(machineTblDao).queryByIpPort(Mockito.eq("ip1"),Mockito.eq(3306));
        MvcResult mvcResult = mvc.perform(get("/api/drc/v1/monitor/uuid?ip=ip1&port=3306")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtils.toJson(machineTbl))
                        .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        Assert.assertEquals(200, mvcResult.getResponse().getStatus());

        Mockito.doThrow(new SQLException()).when(machineTblDao).queryByIpPort(Mockito.eq("ip1"),Mockito.eq(3307));
        mvcResult = mvc.perform(get("/api/drc/v1/monitor/uuid?ip=ip1&port=3307")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtils.toJson(machineTbl))
                        .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_FAIL);
    }

    @Test
    public void UpdateUuidToDBForRemoteDC() throws Exception {
        MachineTbl machineTbl = new MachineTbl();
        machineTbl.setUuid("uuid1");
        Mockito.doReturn(1).when(machineTblDao).update(Mockito.any(MachineTbl.class));
        MvcResult mvcResult = mvc.perform(post("/api/drc/v1/monitor/uuid")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtils.toJson(machineTbl))
                        .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        Assert.assertEquals(200, mvcResult.getResponse().getStatus());

        Mockito.doThrow(new SQLException()).when(machineTblDao).update(Mockito.any(MachineTbl.class));
        mvcResult = mvc.perform(post("/api/drc/v1/monitor/uuid",machineTbl)
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtils.toJson(machineTbl))
                        .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_FAIL);
    }

}
