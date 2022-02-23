package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.dao.entity.MhaGroupTbl;
import com.ctrip.framework.drc.console.service.MhaService;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author maojiawei
 * @version 1.0
 * date: 2020-08-06
 */
public class MhaControllerTest {

    private MockMvc mvc;

    @InjectMocks
    private MhaController mhaController;

    @Mock
    private MhaService mhaService;
    
    @Mock
    private MetaInfoServiceImpl metaInfoService;

    @Before
    public void setUp() throws SQLException {

        List<Map<String,String>> clusterNamePairs = new ArrayList();
        Map<String,String> namePair = new HashMap();
        namePair.put("db", "env");
        clusterNamePairs.add(namePair);

        List<String> dbNames = new ArrayList();
        dbNames.add("db1");
        dbNames.add("db2");

        List<Map<String, Object>> dbsAndDals = new ArrayList();
        Map<String, Object> mha = new HashMap<>();
        mha.put("a","a");
        mha.put("b","b");
        dbsAndDals.add(mha);

        MhaGroupTbl mhaA_BGroup = new MhaGroupTbl();

        /** initialization */
        MockitoAnnotations.initMocks(this);
        /** build mvc env */
        mvc = MockMvcBuilders.standaloneSetup(mhaController).build();

        Mockito.when(mhaService.getCachedAllClusterNames(Mockito.anyString())).thenReturn(clusterNamePairs);
        Mockito.when(mhaService.getCachedAllClusterNames()).thenReturn(clusterNamePairs);
        Mockito.when(mhaService.getAllDbs(Mockito.anyString(), Mockito.anyString())).thenReturn(dbNames);
        Mockito.when(mhaService.getAllDbsAndDals(Mockito.anyString(), Mockito.anyString())).thenReturn(dbsAndDals);
        Mockito.when(mhaService.getAllDbsAndDals(Mockito.anyString(), Mockito.anyString(), Mockito.anyString())).thenReturn(dbsAndDals);
        Mockito.when(metaInfoService.getMhaGroup(Mockito.eq("mhaA"),Mockito.eq("mhaB"))).thenReturn(mhaA_BGroup);
    }

    @Test
    public void testGetRealUuid() throws Throwable {
        try(MockedStatic<MySqlUtils> theMock = Mockito.mockStatic(MySqlUtils.class)) {
            theMock.when(() ->MySqlUtils.getUuid(Mockito.anyString(),Mockito.anyInt(),Mockito.any(),Mockito.any(),Mockito.anyBoolean())).thenReturn("uuidA");
            MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/mha/mhaA,mhaB/uuid/ip1/3306/true")
                            .accept(MediaType.APPLICATION_JSON))
                    .andDo(MockMvcResultHandlers.print())
                    .andReturn();
            int status = mvcResult.getResponse().getStatus();
            String response = mvcResult.getResponse().getContentAsString();
            Assert.assertEquals(200, status);
            System.out.println(response);
            Assert.assertNotNull(response);
            Assert.assertNotEquals("", response);

            Mockito.when(metaInfoService.getMhaGroup(Mockito.eq("mhaA"),Mockito.eq("mhaB"))).thenThrow(new SQLException("test"));
            mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/mha/mhaA,mhaB/uuid/ip1/3306/true")
                            .accept(MediaType.APPLICATION_JSON))
                    .andDo(MockMvcResultHandlers.print())
                    .andReturn();
            status = mvcResult.getResponse().getStatus();
            response = mvcResult.getResponse().getContentAsString();
            Assert.assertEquals(200, status);
            System.out.println(response);
            Assert.assertNotNull(response);

            mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/mha/mhaA,mhaB/gtid/mhaA")
                            .accept(MediaType.APPLICATION_JSON))
                    .andDo(MockMvcResultHandlers.print())
                    .andReturn();
            status = mvcResult.getResponse().getStatus();
            response = mvcResult.getResponse().getContentAsString();
            Assert.assertEquals(200, status);
            System.out.println(response);
            Assert.assertNotNull(response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Test
    public void testGetAllClusterNames() throws Exception {
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/mha/mhanames")
                .accept(MediaType.APPLICATION_JSON))
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
    public void testGetDbNames() throws Exception {
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/mha/dbnames/cluster/clusterName/env/env")
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        System.out.println(response);
        Assert.assertEquals(200, status);
        Assert.assertNotNull(response);
        Assert.assertNotEquals("", response);
    }

    @Test
    public void testGetAllDbsAndDals() throws Exception {
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/mha/dbnames/dalnames/cluster/clusterName/env/env")
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        System.out.println(response);
        Assert.assertEquals(200, status);
        Assert.assertNotNull(response);
        Assert.assertNotEquals("", response);
    }

    @Test
    public void testGetAllDbsAndDals2() throws Exception {
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/mha/dbnames/dalnames/cluster/clusterName/env/env/zoneId/zoneId")
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        System.out.println(response);
        Assert.assertEquals(200, status);
        Assert.assertNotNull(response);
        Assert.assertNotEquals("", response);
    }
}
