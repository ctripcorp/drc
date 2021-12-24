package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.service.MhaService;
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
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

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

    @Before
    public void setUp(){

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


        /** initialization */
        MockitoAnnotations.initMocks(this);
        /** build mvc env */
        mvc = MockMvcBuilders.standaloneSetup(mhaController).build();

        Mockito.when(mhaService.getCachedAllClusterNames(Mockito.anyString())).thenReturn(clusterNamePairs);
        Mockito.when(mhaService.getCachedAllClusterNames()).thenReturn(clusterNamePairs);
        Mockito.when(mhaService.getAllDbs(Mockito.anyString(), Mockito.anyString())).thenReturn(dbNames);
        Mockito.when(mhaService.getAllDbsAndDals(Mockito.anyString(), Mockito.anyString())).thenReturn(dbsAndDals);
        Mockito.when(mhaService.getAllDbsAndDals(Mockito.anyString(), Mockito.anyString(), Mockito.anyString())).thenReturn(dbsAndDals);

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
