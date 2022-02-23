package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.dto.BuildMhaDto;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.console.dto.MhaMachineDto;
import com.ctrip.framework.drc.console.service.impl.AccessServiceImpl;
import com.ctrip.framework.drc.console.service.impl.DrcMaintenanceServiceImpl;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author maojiawei
 * @version 1.0
 * date: 2020-07-28
 */
public class AccessControllerTest extends AbstractControllerTest {

    @InjectMocks
    private AccessController accessController;

    @Mock
    private AccessServiceImpl accessService;

    @Mock
    private DrcMaintenanceServiceImpl drcMaintenanceService;

    @Before
    public void setup() throws Throwable {
        Map<String, Object> res = new HashMap<>();
        res.put("x", 1);
        res.put("y", 2);
        res.put("z", 3);
        /** initialization */
        MockitoAnnotations.initMocks(this);
        /** build mvc env */
        mvc = MockMvcBuilders.standaloneSetup(accessController).build();
        Mockito.when(accessService.buildMhaCluster(Mockito.anyString())).thenReturn(res);
        Mockito.doReturn(res).when(accessService).buildMhaClusterV2(Mockito.anyString());
        Mockito.when(accessService.getCopyResult(Mockito.anyString())).thenReturn(res);
        Mockito.when(accessService.releaseDalCluster(Mockito.anyString(), Mockito.anyString())).thenReturn(res);
        Mockito.when(accessService.registerDalCluster(Mockito.anyString(), Mockito.anyString(), Mockito.anyString())).thenReturn(res);
        Mockito.when(accessService.deployDns(Mockito.anyString())).thenReturn(res);
        Mockito.when(accessService.applyPreCheck(Mockito.anyString())).thenReturn(res);
        
    }

    @Test
    public void testRecordMachineInfo() throws Throwable {
        Mockito.when(drcMaintenanceService.recordMhaInstances(Mockito.any(MhaInstanceGroupDto.class))).thenReturn(true);
        MhaMachineDto dto = new MhaMachineDto();
        dto.setMhaName("mhaA");
        dto.setMaster(true);
        dto.setMySQLInstance(new MhaInstanceGroupDto.MySQLInstance());
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/access/mha/machineInfo")
                .contentType(MediaType.APPLICATION_JSON).content(getRequestBody(dto)).accept(MediaType.APPLICATION_JSON)).andReturn();
        
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
        Assert.assertNotNull(response);
        Assert.assertNotEquals("", response);

        
        Mockito.when(drcMaintenanceService.recordMhaInstances(Mockito.any(MhaInstanceGroupDto.class))).thenThrow(new SQLException("testSqlException"));
        dto.setMaster(false);
        mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/access/mha/machineInfo")
                .contentType(MediaType.APPLICATION_JSON).content(getRequestBody(dto)).accept(MediaType.APPLICATION_JSON)).andReturn();
        Assert.assertEquals(200, status);
        response = mvcResult.getResponse().getContentAsString();
        System.out.println(response);
        Assert.assertNotNull(response);
        Assert.assertNotEquals("", response);
    }
    
    @Test
    public void testPreCheck() throws Exception {
        String requestBody = "{\n" +
                "  \"clustername\": \"clusterName\"\n" +
                "}";
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/access/precheck")
                        .contentType(MediaType.APPLICATION_JSON).content(requestBody)
                        .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
        Assert.assertNotNull(response);
        Assert.assertNotEquals("", response);
    }

    @Test
    public void testBuild() throws Exception {
        String requestBody = "{\n" +
                "  \"clustername\": \"clusterName\",\n" +
                "  \"drczone\": \"shajq\"\n" +
                "}";
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/access/mha")
                        .contentType(MediaType.APPLICATION_JSON).content(requestBody)
                        .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
        Assert.assertNotNull(response);
        Assert.assertNotEquals("", response);
    }

    @Test
    public void testBuildV2() throws Exception {
        String requestBody = "{\n" +
                "  \"clustername\": \"clusterName\",\n" +
                "  \"drczone\": \"shajq\"\n" +
                "}";
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/access/mhaV2")
                        .contentType(MediaType.APPLICATION_JSON).content(requestBody)
                        .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_SUCCESS);

        Mockito.doThrow(new Exception("")).when(accessService).buildMhaClusterV2(Mockito.anyString());
        mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/access/mhaV2")
                        .contentType(MediaType.APPLICATION_JSON).content(requestBody)
                        .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_FAIL);
    }

    @Test
    public void testStandaloneBuildMhaGroup() throws Exception {
        BuildMhaDto buildMhaDto = new BuildMhaDto().setBuName("bu").setDalClusterName("dalcluster").setAppid(1234L).setOriginalMha("ori").setNewBuiltMha("newMha").setNewBuiltMhaDc("newDc");
        Mockito.doReturn(ApiResult.getSuccessInstance(true)).when(accessService).initMhaGroup(Mockito.any());
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/access/mha/standalone")
                        .contentType(MediaType.APPLICATION_JSON).content(getRequestBody(buildMhaDto))
                        .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        assertNormalResponse(mvcResult);
    }

    @Test
    public void testStandAloneBuildMachine() throws Throwable {
        Mockito.doReturn(true).when(drcMaintenanceService).updateMhaInstances(Mockito.any(), Mockito.eq(false));
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/access/machine/standalone")
                        .contentType(MediaType.APPLICATION_JSON).content(getRequestBody(new MhaInstanceGroupDto()))
                        .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_SUCCESS);

        Mockito.doThrow(new Throwable()).when(drcMaintenanceService).updateMhaInstances(Mockito.any(), Mockito.eq(false));
        mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/access/machine/standalone")
                        .contentType(MediaType.APPLICATION_JSON).content(getRequestBody(new MhaInstanceGroupDto()))
                        .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_FAIL);
    }

    @Test
    public void testStopCheckNewMhaBuilt() throws Exception {
        Mockito.doReturn(ApiResult.getSuccessInstance(true)).when(accessService).stopCheckNewMhaBuilt(Mockito.anyString());
        MvcResult mvcResult = doNormalDelete("/api/drc/v1/access/mhas/testMha");
        assertNormalResponse(mvcResult);
    }

    @Test
    public void testCopy() throws Exception {
        String requestBody1 = "{\n" +
                "  \"OriginalCluster\": \"dbatmptest4\",\n" +
                "  \"DrcCluster\": \"dbatmptest4rb\",\n" +
                "  \"rplswitch\": 0\n" +
                "}";
        String requestBody2 = "{\n" +
                "  \"OriginalCluster\": \"dbatmptest4\",\n" +
                "  \"DrcCluster\": \"dbatmptest4rb\",\n" +
                "  \"rplswitch\": 1\n" +
                "}";
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/access/copystatus")
                        .contentType(MediaType.APPLICATION_JSON).content(requestBody2)
                        .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
        Assert.assertNotNull(response);
        Assert.assertNotEquals("", response);
    }

    @Test
    public void testDns() throws Exception {
        String requestBody = "{\n" +
                "  \"cluster\": \"clusterName\",\n" +
                "  \"dbnames\": \"dbName\",\n" +
                "  \"env\": \"product\",\n" +
                "  \"needread\": 0\n" +
                "}";
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/access/dns")
                        .contentType(MediaType.APPLICATION_JSON).content(requestBody)
                        .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
        Assert.assertNotNull(response);
        Assert.assertNotEquals("", response);
    }

    @Test
    public void testDal() throws Exception {
        String requestBody = "{\n" +
                "  \"dalclustername\": \"HtlInputRequestDB_dalcluster\",\n" +
                "  \"dbname\": \"HtlInputRequestDB\",\n" +
                "  \"clustername\": \"fatpub2\",\n" +
                "  \"isshard\": false,\n" +
                "  \"shardstartno\": 0\n" +
                "}";
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/access/clusters/env/fat/goal/instance")
                        .contentType(MediaType.APPLICATION_JSON).content(requestBody)
                        .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
        Assert.assertNotNull(response);
        Assert.assertNotEquals("", response);
    }

    @Test
    public void testDalRelease() throws Exception {
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/access/clusters/clustername/bbzdrccameldb_dalcluster/env/fat/releases")
                        .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
        Assert.assertNotNull(response);
        Assert.assertNotEquals("", response);
    }
}
