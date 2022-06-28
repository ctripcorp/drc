package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.MhaTblDao;
import com.ctrip.framework.drc.console.dao.entity.MhaGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.service.MhaService;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
import java.util.*;

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
    
    @Mock
    private DalUtils dalUtils;
    
    @Mock
    private DefaultConsoleConfig consoleConfig;
    

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

            mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/mha/mhaA,mhaB/uuid/ip1/3306/true")
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
    
    @Test
    public void testGetGtd() throws Exception {
        MhaTbl mhaTbl = new MhaTbl();
        mhaTbl.setDcId(1L);
        Map<String, String> consoleDcInfos = Maps.newHashMap();
        consoleDcInfos.put("publicDc","domain");
        HashSet<String> publicDc = Sets.newHashSet("publicDc");
        
        MySqlEndpoint mySqlEndpoint = new MySqlEndpoint("ip1", 3306, "usr", "psw", true);
        Mockito.when(dalUtils.queryByMhaName(Mockito.anyString(),Mockito.any())).thenReturn(mhaTbl);
        Mockito.when(dalUtils.getDcNameByDcId(Mockito.eq(1L))).thenReturn("publicDc");
        Mockito.when(metaInfoService.getMasterEndpoint(Mockito.any(MhaTbl.class))).thenReturn(mySqlEndpoint);
        Mockito.when(consoleConfig.getConsoleDcInfos()).thenReturn(consoleDcInfos);
        Mockito.when(consoleConfig.getPublicCloudDc()).thenReturn(publicDc);

        try(MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when(() -> HttpUtils.get(
                    Mockito.eq("domain//api/drc/v1/local/gtid?" +
                            "mha=" + "mha1" +
                            "&ip=" + "ip1" +
                            "&port=" + 3306 +
                            "&user=" + "usr" +
                            "&psw=" + "psw "))).thenReturn(ApiResult.getSuccessInstance("gtidString"));
            MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/mha/gtid/mha1,mha2/mha1" )
                            .accept(MediaType.APPLICATION_JSON))
                    .andDo(MockMvcResultHandlers.print())
                    .andReturn();
            int status = mvcResult.getResponse().getStatus();
            String response = mvcResult.getResponse().getContentAsString();
            Assert.assertEquals(200, status);
            System.out.println(response);
        } catch (Exception e) {
            e.printStackTrace();
        }


        Mockito.when(dalUtils.getDcNameByDcId(Mockito.eq(1L))).thenReturn("notPublicDc");
        try(MockedStatic<MySqlUtils> theMock = Mockito.mockStatic(MySqlUtils.class)) {
            theMock.when(() -> MySqlUtils.getUnionExecutedGtid(Mockito.any())).
                    thenReturn("GtidSetString");
            MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/mha/gtid/mha1,mha2/mha1" )
                            .accept(MediaType.APPLICATION_JSON))
                    .andDo(MockMvcResultHandlers.print())
                    .andReturn();
            int status = mvcResult.getResponse().getStatus();
            String response = mvcResult.getResponse().getContentAsString();
            Assert.assertEquals(200, status);
            System.out.println(response);


            Mockito.when(dalUtils.getDcNameByDcId(Mockito.eq(1L))).thenThrow(new SQLException("sql erroe"));
            mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/mha/gtid/mha1,mha2/mha1" )
                            .accept(MediaType.APPLICATION_JSON))
                    .andDo(MockMvcResultHandlers.print())
                    .andReturn();
            status = mvcResult.getResponse().getStatus();
            response = mvcResult.getResponse().getContentAsString();
            Assert.assertEquals(200, status);
            System.out.println(response);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        
    }
}
