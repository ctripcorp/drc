package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.dto.MetaProposalDto;
import com.ctrip.framework.drc.console.dto.ProxyDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.EstablishStatusEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.impl.*;
import com.ctrip.framework.drc.console.vo.MhaGroupPair;
import com.ctrip.framework.drc.console.vo.MhaGroupPairVo;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.fasterxml.jackson.databind.JsonNode;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.ctrip.framework.drc.console.AllTests.DRC_XML;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-08-11
 */
public class MetaControllerTest extends AbstractControllerTest {

    @InjectMocks
    private MetaController controller;

    @Mock
    private TransferServiceImpl transferService;

    @Mock
    private MetaInfoServiceImpl metaInfoService;

    @Mock
    private DrcMaintenanceServiceImpl drcMaintenanceService;

    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Mock
    private DrcBuildServiceImpl drcBuildService;

    @Mock
    private MetaInfoServiceTwoImpl metaInfoServiceTwo;

    @Mock
    private MetaGenerator metaService;

    @Mock
    private DbClusterSourceProvider sourceProvider;

    private static final String META_DATA = "{\"metaData\":\"data\"}";

    private static final String DC = "shaoy";

    private Drc drcInDc1;

    private Drc drc;

    private static final String IP = "ip";

    private static final String MHA = "testmha";

    private MetaProposalDto metaProposalDto;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        mvc = MockMvcBuilders.standaloneSetup(controller).build();
        Mockito.doAnswer((o) -> null).when(transferService).loadMetaData(META_DATA);

        drc = DefaultSaxParser.parse(DRC_XML);
        drcInDc1 = new Drc();
        drcInDc1.addDc(drc.findDc("dc1"));
        Mockito.doReturn(drcInDc1).when(sourceProvider).getDrc("dc1");
        Mockito.doReturn(drc).when(metaService).getDrc();

        List<String> rResources = new ArrayList<>() {{
            add("ip1");
            add("ip2");
        }};
        List<String> aResources = new ArrayList<>() {{
            add("ip1");
            add("ip2");
            add("ip3");
        }};
        List<Integer> instances = new ArrayList<>() {{
           add(8888);
        }};
        Mockito.when(metaInfoService.getResourcesInDcOfMha(MHA, "R")).thenReturn(rResources);
        Mockito.when(metaInfoService.getResourcesInUse(MHA, "", "R")).thenReturn(rResources);
        doReturn(rResources).when(metaInfoService).getReplicatorResources(DC);
        doReturn(aResources).when(metaInfoService).getApplierResources(DC);
        doReturn(rResources).when(metaInfoServiceTwo).getResources(DC, "R");
        doReturn(aResources).when(metaInfoServiceTwo).getResources(DC, "A");
        doReturn(instances).when(metaInfoService).getReplicatorInstances(IP);

        metaProposalDto = new MetaProposalDto();
        metaProposalDto.setSrcMha("drcOy");
        metaProposalDto.setDestMha("drcRb");
        metaProposalDto.setSrcReplicatorIps(new ArrayList<>() {{
            add("10.2.83.105");
        }});
        metaProposalDto.setDestReplicatorIps(new ArrayList<>() {{
            add("10.2.83.105");
            add("10.2.86.199");
        }});
        metaProposalDto.setSrcApplierIps(new ArrayList<>() {{
            add("10.2.83.100");
            add("10.2.86.137");
        }});
        metaProposalDto.setDestApplierIps(new ArrayList<>() {{
            add("10.2.83.111");
            add("10.2.86.136");
        }});
    }

//    @Test
//    public void testGetAllMhaGroups() throws Exception {
//        List<MhaGroupPair> mhaGroupPairs = new ArrayList<>() {{
//            add(new MhaGroupPair("mha1", "mha2", EstablishStatusEnum.BUILT_NEW_MHA, 0, 0, 0L));
//            add(new MhaGroupPair("mha4", "mha3", EstablishStatusEnum.CONFIGURED_DB_DOMAIN_NAME, 0, 0, 0L));
//            add(new MhaGroupPair("mha5", "mha6", EstablishStatusEnum.ESTABLISHED, 0, 0 ,0L));
//        }};
//
//        when(metaInfoService.getAllMhaGroups()).thenReturn(mhaGroupPairs);
//        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/meta/groups/all/"))
//                .andDo(MockMvcResultHandlers.print())
//                .andReturn();
//        MockHttpServletResponse response = mvcResult.getResponse();
//        int status = response.getStatus();
//        String responseStr = response.getContentAsString();
//        Assert.assertEquals(200, status);
//        JsonNode jsonNode = objectMapper.readTree(responseStr);
//        Assert.assertNotEquals("null", jsonNode.get("data").toString());
//
//        when(metaInfoService.getAllMhaGroups()).thenThrow(Exception.class);
//        mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/meta/groups/all/"))
//                .andDo(MockMvcResultHandlers.print())
//                .andReturn();
//        response = mvcResult.getResponse();
//        status = response.getStatus();
//        responseStr = response.getContentAsString();
//        Assert.assertEquals(200, status);
//        jsonNode = objectMapper.readTree(responseStr);
//        Assert.assertEquals(1, jsonNode.get("status").asInt());
//        Assert.assertEquals("null", jsonNode.get("data").toString());
//    }

    @Test
    public void testGetAllOrderedGroupsAfterFilter() throws Exception {
        List<MhaGroupPairVo> mhaGroupPairVos = new ArrayList<>() {{
            add(new MhaGroupPairVo("mha1", "mha2", EstablishStatusEnum.BUILT_NEW_MHA.getCode(), 0, 0, 0L));
            add(new MhaGroupPairVo("mha1", "mha3", EstablishStatusEnum.CONFIGURED_DB_DOMAIN_NAME.getCode(), 0, 0, 0L));
            add(new MhaGroupPairVo("mha5", "mha6", EstablishStatusEnum.ESTABLISHED.getCode(), 0, 0, 0L));
        }};

        when(metaInfoService.getMhaGroupPariVos(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any()))
                .thenReturn(mhaGroupPairVos);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/meta/orderedGroups/all/?deleted=0"))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        MockHttpServletResponse response = mvcResult.getResponse();
        int status = response.getStatus();
        String responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        JsonNode jsonNode = objectMapper.readTree(responseStr);
        Assert.assertNotEquals("null", jsonNode.get("data").toString());
        
        mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/meta/orderedGroups/all/?deleted=1"))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        response = mvcResult.getResponse();
        status = response.getStatus();
        responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        jsonNode = objectMapper.readTree(responseStr);
        Assert.assertEquals("null", jsonNode.get("data").toString());

        when(metaInfoService.getMhaGroupPariVos(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any())).thenThrow(new SQLException("sql error"));
        mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/meta/orderedGroups/all/?deleted=1"))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        response = mvcResult.getResponse();
        status = response.getStatus();
        responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        jsonNode = objectMapper.readTree(responseStr);
        Assert.assertEquals("null", jsonNode.get("data").toString());
    }

    @Test
    public void testLoadMetaData() throws Exception {
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.put("/api/drc/v1/meta/").contentType(MediaType.APPLICATION_JSON_UTF8).content(META_DATA))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        MockHttpServletResponse response = mvcResult.getResponse();
        int status = response.getStatus();
        String responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        JsonNode jsonNode = objectMapper.readTree(responseStr);
        Assert.assertEquals("true", jsonNode.get("data").toString());
    }

    @Test
    public void testLoadOneMetaData() throws Exception {
        Mockito.doAnswer((o) -> null).when(transferService).loadOneMetaData(META_DATA);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.put("/api/drc/v1/meta/one").contentType(MediaType.APPLICATION_JSON_UTF8).content(META_DATA))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        MockHttpServletResponse response = mvcResult.getResponse();
        int status = response.getStatus();
        String responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        JsonNode jsonNode = objectMapper.readTree(responseStr);
        Assert.assertEquals("true", jsonNode.get("data").toString());

        Mockito.doThrow(new Exception("fail load one")).when(transferService).loadOneMetaData(META_DATA);
        mvcResult = mvc.perform(MockMvcRequestBuilders.put("/api/drc/v1/meta/one").contentType(MediaType.APPLICATION_JSON_UTF8).content(META_DATA))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        response = mvcResult.getResponse();
        status = response.getStatus();
        responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        jsonNode = objectMapper.readTree(responseStr);
        Assert.assertEquals("false", jsonNode.get("data").toString());
    }

    @Test
    public void testAutoLoadMetaData() throws Exception {
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.put("/api/drc/v1/meta/auto"))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        MockHttpServletResponse response = mvcResult.getResponse();
        int status = response.getStatus();
        String responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        JsonNode jsonNode = objectMapper.readTree(responseStr);
        Assert.assertEquals("true", jsonNode.get("data").toString());
    }

    @Test
    public void testChangeGroupStatus() throws Exception {
        MhaGroupPair mhaGroupPair = new MhaGroupPair();
        mhaGroupPair.setSrcMha("testMha1");
        mhaGroupPair.setDestMha("testMha2");
        String jsonStr = objectMapper.writeValueAsString(mhaGroupPair);
        when(drcMaintenanceService.changeMhaGroupStatus(any(), any())).thenReturn(true);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/meta/group/status/60").contentType(MediaType.APPLICATION_JSON).content(jsonStr))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        MockHttpServletResponse response = mvcResult.getResponse();
        int status = response.getStatus();
        String responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        JsonNode jsonNode = objectMapper.readTree(responseStr);
        Assert.assertEquals("true", jsonNode.get("data").toString());

        when(drcMaintenanceService.changeMhaGroupStatus(any(), any())).thenThrow(Exception.class);
        mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/meta/group/status/60").contentType(MediaType.APPLICATION_JSON).content(jsonStr))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        response = mvcResult.getResponse();
        status = response.getStatus();
        responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        jsonNode = objectMapper.readTree(responseStr);
        Assert.assertEquals("false", jsonNode.get("data").toString());
    }

    @Test
    public void testGetAllMetaData() throws Exception {
        Mockito.doReturn(DRC_XML).when(sourceProvider).getDrcString();
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/meta/"))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        MockHttpServletResponse response = mvcResult.getResponse();
        int status = response.getStatus();
        String responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        Drc actual = DefaultSaxParser.parse(responseStr);
        Assert.assertEquals(drc.toString(), actual.toString());
    }

    @Test
    public void testGetLocalDrcStr() throws Exception {
        Mockito.doReturn(drcInDc1).when(sourceProvider).getLocalDrc();
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/meta/local"))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        MockHttpServletResponse response = mvcResult.getResponse();
        int status = response.getStatus();
        String responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        Drc actual = DefaultSaxParser.parse(responseStr);
        Assert.assertEquals(drcInDc1.toString(), actual.toString());
    }

    @Test
    public void testGetDrcStr() throws Exception {
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/meta/data/dcs/dc1"))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        MockHttpServletResponse response = mvcResult.getResponse();
        int status = response.getStatus();
        String responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        Drc actual = DefaultSaxParser.parse(responseStr);
        Assert.assertEquals(drcInDc1.toString(), actual.toString());
    }

    @Test
    public void testGetIncludedDbs() throws Exception {
        Mockito.doReturn("db1,db2").when(metaInfoService).getIncludedDbs("testMha", "testRemoteMha");
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/meta/includeddbs?localMha=testMha&remoteMha=testRemoteMha"))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        MockHttpServletResponse response = mvcResult.getResponse();
        int status = response.getStatus();
        String responseStr = response.getContentAsString();
        ApiResult result = objectMapper.readValue(responseStr, ApiResult.class);
        Assert.assertEquals(0, result.getStatus().intValue());
        Assert.assertEquals("db1,db2", result.getData().toString());

        Mockito.doThrow(new SQLException()).when(metaInfoService).getIncludedDbs("testMha", "testRemoteMha");
        mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/meta/includeddbs?localMha=testMha&remoteMha=testRemoteMha"))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        response = mvcResult.getResponse();
        status = response.getStatus();
        responseStr = response.getContentAsString();
        result = objectMapper.readValue(responseStr, ApiResult.class);
        Assert.assertEquals(0, result.getStatus().intValue());
        Assert.assertEquals(null, result.getData());
    }

    @Test
    public void testGetResourcesInDcOfMha() throws Exception {
        String uri = String.format("/api/drc/v1/meta/mhas/%s/resources/all/types/%s", MHA, "R");
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get(uri))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        MockHttpServletResponse response = mvcResult.getResponse();
        int status = response.getStatus();
        String responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        JsonNode jsonNode = objectMapper.readTree(responseStr);
        JsonNode dataNode = jsonNode.get("data");
        System.out.println(objectMapper.writeValueAsString(dataNode));
        Assert.assertEquals(2, dataNode.size());
    }

    @Test
    public void testGetResourcesInUse() throws Exception {
        String uri = String.format("/api/drc/v1/meta/resources/using/types/%s?localMha=%s", "R", MHA);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get(uri))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        MockHttpServletResponse response = mvcResult.getResponse();
        int status = response.getStatus();
        String responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        JsonNode jsonNode = objectMapper.readTree(responseStr);
        JsonNode dataNode = jsonNode.get("data");
        System.out.println(objectMapper.writeValueAsString(dataNode));
        Assert.assertEquals(2, dataNode.size());
    }

    @Test
    public void testGetResources() throws Exception {
        String uri = String.format("/api/drc/v1/meta/dcs/%s/resources/types/%s", DC, "R");
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get(uri))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        MockHttpServletResponse response = mvcResult.getResponse();
        int status = response.getStatus();
        String responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        JsonNode jsonNode = objectMapper.readTree(responseStr);
        Assert.assertEquals(2, jsonNode.get("data").size());

        uri = String.format("/api/drc/v1/meta/dcs/%s/resources/types/%s", DC, "A");
        mvcResult = mvc.perform(MockMvcRequestBuilders.get(uri))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        response = mvcResult.getResponse();
        status = response.getStatus();
        responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        jsonNode = objectMapper.readTree(responseStr);
        Assert.assertEquals(3, jsonNode.get("data").size());
    }

    @Test
    public void testGetInstances() throws Exception {
        String uri = String.format("/api/drc/v1/meta/resources/ip/%s", IP);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get(uri))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        MockHttpServletResponse response = mvcResult.getResponse();
        int status = response.getStatus();
        String responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        JsonNode jsonNode = objectMapper.readTree(responseStr);
        Assert.assertEquals(1, jsonNode.get("data").size());
    }

    @Test
    public void testSubmitConfig() throws Exception {
        String jsonStr = objectMapper.writeValueAsString(metaProposalDto);
        when(drcBuildService.submitConfig(metaProposalDto)).thenReturn(META_DATA);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/meta/config").contentType(MediaType.APPLICATION_JSON).content(jsonStr))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        MockHttpServletResponse response = mvcResult.getResponse();
        int status = response.getStatus();
        String responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        JsonNode jsonNode = objectMapper.readTree(responseStr);
        Assert.assertEquals(META_DATA, jsonNode.get("data").asText());


        when(drcBuildService.submitConfig(metaProposalDto)).thenThrow(Exception.class);
        mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/meta/config").contentType(MediaType.APPLICATION_JSON).content(jsonStr))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        response = mvcResult.getResponse();
        status = response.getStatus();
        responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        jsonNode = objectMapper.readTree(responseStr);
        Assert.assertEquals("null", jsonNode.get("data").toString());
    }

    @Test
    public void testRemoveConfig() throws Exception {
        Mockito.doNothing().when(transferService).removeConfig(anyString(), anyString());
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.delete("/api/drc/v1/meta/config/remove/mhas/testMha1,testMha2"))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        MockHttpServletResponse response = mvcResult.getResponse();
        int status = response.getStatus();
        String responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        JsonNode jsonNode = objectMapper.readTree(responseStr);
        Assert.assertEquals("true", jsonNode.get("data").toString());

        Mockito.doThrow(Exception.class).when(transferService).removeConfig(anyString(), anyString());
        mvcResult = mvc.perform(MockMvcRequestBuilders.delete("/api/drc/v1/meta/config/remove/mhas/testMha1,testMha2"))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        response = mvcResult.getResponse();
        status = response.getStatus();
        responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        jsonNode = objectMapper.readTree(responseStr);
        Assert.assertEquals("false", jsonNode.get("data").toString());
    }

    @Test
    public void testGetConfig() throws Exception {
        Mockito.doReturn(META_DATA).when(metaInfoService).getXmlConfiguration(anyString(), anyString());
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/meta/config/mhas/testMha1,testMha2"))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        MockHttpServletResponse response = mvcResult.getResponse();
        int status = response.getStatus();
        String responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        ApiResult result = objectMapper.readValue(responseStr, ApiResult.class);
        Assert.assertEquals(0, result.getStatus().intValue());
        Assert.assertEquals(META_DATA, result.getData().toString());

        Mockito.doThrow(Exception.class).when(metaInfoService).getXmlConfiguration(anyString(), anyString());
        mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/meta/config/mhas/testMha1,testMha2"))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        response = mvcResult.getResponse();
        status = response.getStatus();
        responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        result = objectMapper.readValue(responseStr, ApiResult.class);
        Assert.assertEquals(1, result.getStatus().intValue());
        Assert.assertNull(result.getData());
    }

    @Test
    public void testInputResource() throws Exception {
        Mockito.when(drcMaintenanceService.inputResource(anyString(), anyString(), anyString())).thenReturn(true);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/meta/resource/ips/127.0.0.1/dcs/shaoy/descriptions/R"))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        MockHttpServletResponse response = mvcResult.getResponse();
        int status = response.getStatus();
        String responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        ApiResult result = objectMapper.readValue(responseStr, ApiResult.class);
        Assert.assertEquals(0, result.getStatus().intValue());
        Assert.assertEquals(true, result.getData());

        Mockito.when(drcMaintenanceService.inputResource(anyString(), anyString(), anyString())).thenThrow(new Exception("unexpected exception"));
        mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/meta/resource/ips/127.0.0.1/dcs/shaoy/descriptions/R"))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        response = mvcResult.getResponse();
        status = response.getStatus();
        responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        result = objectMapper.readValue(responseStr, ApiResult.class);
        Assert.assertEquals(1, result.getStatus().intValue());
        Assert.assertNotNull(result.getData().toString());
        System.out.println(result.getData().toString());
    }

    @Test
    public void testDeleteResource() throws Exception {
        Mockito.when(drcMaintenanceService.deleteResource(anyString())).thenReturn(1);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.delete("/api/drc/v1/meta/resource/ips/127.0.0.1"))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        MockHttpServletResponse response = mvcResult.getResponse();
        int status = response.getStatus();
        String responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        ApiResult result = objectMapper.readValue(responseStr, ApiResult.class);
        Assert.assertEquals(0, result.getStatus().intValue());
        Assert.assertEquals(1, result.getData());

        Mockito.when(drcMaintenanceService.deleteResource(anyString())).thenThrow(new Exception("unexpected exception"));
        mvcResult = mvc.perform(MockMvcRequestBuilders.delete("/api/drc/v1/meta/resource/ips/127.0.0.1"))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        response = mvcResult.getResponse();
        status = response.getStatus();
        responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        result = objectMapper.readValue(responseStr, ApiResult.class);
        Assert.assertEquals(1, result.getStatus().intValue());
        Assert.assertNotNull(result.getData().toString());
        System.out.println(result.getData().toString());
    }

    @Test
    public void testDeleteMachine() throws Exception {
        Mockito.when(drcMaintenanceService.deleteMachine(anyString())).thenReturn(1);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.delete("/api/drc/v1/meta/machine/ips/127.0.0.1"))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        MockHttpServletResponse response = mvcResult.getResponse();
        int status = response.getStatus();
        String responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        ApiResult result = objectMapper.readValue(responseStr, ApiResult.class);
        Assert.assertEquals(0, result.getStatus().intValue());
        Assert.assertEquals(1, result.getData());

        Mockito.when(drcMaintenanceService.deleteMachine(anyString())).thenThrow(new Exception("unexpected exception"));
        mvcResult = mvc.perform(MockMvcRequestBuilders.delete("/api/drc/v1/meta/machine/ips/127.0.0.1"))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        response = mvcResult.getResponse();
        status = response.getStatus();
        responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        result = objectMapper.readValue(responseStr, ApiResult.class);
        Assert.assertEquals(1, result.getStatus().intValue());
        Assert.assertNotNull(result.getData().toString());
        System.out.println(result.getData().toString());
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
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/meta/dcs/testDc"))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        assertNormalResponse(mvcResult);
    }

    @Test
    public void testInputBu() throws Exception {
        Mockito.doReturn(ApiResult.getSuccessInstance(true)).when(drcMaintenanceService).inputBu(anyString());
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/meta/orgs/testOrg"))
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

    @Test
    public void testInputGroupMapping() throws Exception {
        Mockito.doReturn(1L).when(drcMaintenanceService).inputGroupMapping(1L, 1L);
        MvcResult mvcResult = doNormalPost("/api/drc/v1/meta/group/mapping/group/1/mha/1");
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_SUCCESS);

        Mockito.doThrow(new SQLException()).when(drcMaintenanceService).inputGroupMapping(1L, 1L);
        mvcResult = doNormalPost("/api/drc/v1/meta/group/mapping/group/1/mha/1");
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_FAIL);
    }

    @Test
    public void testDeleteGroupMapping() throws Exception {
        Mockito.doReturn(1).when(drcMaintenanceService).deleteGroupMapping(1L, 1L);
        MvcResult mvcResult = doNormalDelete("/api/drc/v1/meta/group/mapping/group/1/mha/1");
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_SUCCESS);

        Mockito.doThrow(new SQLException()).when(drcMaintenanceService).deleteGroupMapping(1L, 1L);
        mvcResult = doNormalDelete("/api/drc/v1/meta/group/mapping/group/1/mha/1");
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_FAIL);
    }

    @Test
    public void testQueryMhas() throws Exception {
        MhaTbl mhaTbl = new MhaTbl();
        List<MhaTbl> mhaTbls = Lists.newArrayList(mhaTbl);
        Mockito.doReturn(mhaTbls).when(metaInfoService).getMhas("dc1");
        MvcResult mvcResult = doNormalGet("/api/drc/v1/meta/mhas?dcName=dc1");
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_SUCCESS);

        Mockito.doThrow(new SQLException()).when(metaInfoService).getMhas("dc1");
        mvcResult = doNormalGet("/api/drc/v1/meta/mhas?dcName=dc1");
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_FAIL);
    }

    @Test
    public void testGetDeletedConfig() throws Exception {
        Mockito.doReturn(null).when(metaInfoService).getXmlConfiguration("mha1","mha2", BooleanEnum.TRUE);
        MvcResult mvcResult = doNormalGet("/api/drc/v1/meta/config/deletedMhas/mha1,mha2");
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_SUCCESS);

        Mockito.doThrow(new SQLException()).when(metaInfoService).getXmlConfiguration("mha1","mha2", BooleanEnum.TRUE);
        mvcResult = doNormalGet("/api/drc/v1/meta/config/deletedMhas/mha1,mha2");
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_FAIL);
    }

    @Test
    public void testDecoverDrc() throws Exception {
        Mockito.doNothing().when(transferService).recoverDeletedDrc("mha1","mha2");
        MvcResult mvcResult = doNormalPost("/api/drc/v1/meta/config/recoverMhas/mha1,mha2");
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_SUCCESS);

        Mockito.doThrow(new SQLException()).when(transferService).recoverDeletedDrc("mha1","mha2");
        mvcResult = doNormalPost("/api/drc/v1/meta/config/recoverMhas/mha1,mha2");
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_FAIL);
    }

    @Test
    @GetMapping("orderedDeletedGroups/all")
    public void testGetAllDeletedMhaGroups() throws Exception {
        Mockito.doReturn(null).when(metaInfoService).getDeletedMhaGroupPairVos();
        MvcResult mvcResult = doNormalGet("/api/drc/v1/meta/orderedDeletedGroups/all");
        assertNormalResponseWithoutCheckingData(mvcResult,ResultCode.HANDLE_SUCCESS);

        Mockito.doThrow(new SQLException()).when(metaInfoService).getDeletedMhaGroupPairVos();
        mvcResult = doNormalGet("/api/drc/v1/meta/orderedDeletedGroups/all");
        assertNormalResponseWithoutCheckingData(mvcResult, ResultCode.HANDLE_FAIL);
    }

    @After
    public void tearDown() throws Exception {
    }


}