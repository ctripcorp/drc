package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.service.LocalService;
import com.ctrip.framework.drc.console.service.RowsFilterService;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.TableCheckVo;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;


public class LocalControllerTest extends AbstractControllerTest {
    
    @InjectMocks
    private LocalController localController;

    @Mock
    private DbClusterSourceProvider dbClusterSourceProvider;

    @Mock
    private RowsFilterService rowsFilterService;
    
    @Mock
    private LocalService localService;

    @Before
    public void setUp() throws SQLException {
        /** initialization */
        MockitoAnnotations.openMocks(this);
        /** build mvc env */
        mvc = MockMvcBuilders.standaloneSetup(localController).build();
        Mockito.when(dbClusterSourceProvider.getMasterEndpoint(Mockito.anyString())).
                thenReturn(new MySqlEndpoint("ip",0,"user","psw",true));
    }

    @Test
    public void testGetMatchTable() {
        try(MockedStatic<MySqlUtils> theMock = Mockito.mockStatic(MySqlUtils.class)) {
            List<MySqlUtils.TableSchemaName> res = 
                    Lists.newArrayList(new MySqlUtils.TableSchemaName("db", "table"));
            theMock.when((MockedStatic.Verification) MySqlUtils.getTablesAfterRegexFilter(Mockito.any(),Mockito.any())).
                    thenReturn(res);
            MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/local/dataMedia/check?" +
                                    "namespace=" + "db" +
                                    "&name=" + "table" +
                                    "&srcDc=" + "dc1" +
                                    "&dataMediaSourceName=" + "mha" +
                                    "&type=" + 0)
                            .accept(MediaType.APPLICATION_JSON))
                    .andDo(MockMvcResultHandlers.print())
                    .andReturn();
            int status = mvcResult.getResponse().getStatus();
            String response = mvcResult.getResponse().getContentAsString();
            Assert.assertEquals(200, status);
            System.out.println(response);

            mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/local/dataMedia/check?" +
                                    "namespace=" + "；" +
                                    "&name=" + "table" +
                                    "&srcDc=" + "dc1" +
                                    "&dataMediaSourceName=" + "mha" +
                                    "&type=" + 0)
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

    @Test
    public void testGetCommonColumnInDataMedias() {
        try(MockedStatic<MySqlUtils> theMock = Mockito.mockStatic(MySqlUtils.class)) {
            Set<String> columns = Sets.newHashSet("columnA");
            theMock.when((MockedStatic.Verification) MySqlUtils.getAllCommonColumns(Mockito.any(),Mockito.any())).
                    thenReturn(columns);
            MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/local/rowsFilter/commonColumns?" +
                                    "srcDc=" + "srcDc" +
                                    "&srcMha=" + "srcMha" +
                                    "&namespace=" + "namespace" +
                                    "&name=" + "name")
                            .accept(MediaType.APPLICATION_JSON))
                    .andDo(MockMvcResultHandlers.print())
                    .andReturn();
            int status = mvcResult.getResponse().getStatus();
            String response = mvcResult.getResponse().getContentAsString();
            Assert.assertEquals(200, status);
            System.out.println(response);

            mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/local/rowsFilter/commonColumns?" +
                                    "srcDc=" + "srcDc" +
                                    "&srcMha=" + "srcMha" +
                                    "&namespace=" + "namespace" +
                                    "&name=" + "name")
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

    @Test
    public void testGetConflictTables() throws Exception {
        Mockito.when(rowsFilterService.getConflictTables(Mockito.anyString(), Mockito.anyList())).
                thenReturn(Lists.newArrayList("conflictTable1"));
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/local/dataMedia/conflictCheck?" +
                                "mhaName=" + "mhaName" +
                                "&logicalTables=" + "db1\\.t1,db2.t2")
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
    }

    @Test
    public void testGetTablesWithoutColumn() throws Exception {
        Mockito.when(rowsFilterService.getTablesWithoutColumn(
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyString())).thenReturn(Lists.newArrayList("table1WithoutColumnA"));

        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/local/dataMedia/columnCheck?" +
                                "srcDc=" + "srcDc" +
                                "&mhaName=" + "mhaName" +
                                "&namespace=" + "namespace" +
                                "&name=" + "name" +
                                "&column=" + "columnA")
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);

        mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/local/dataMedia/columnCheck?" +
                                "srcDc=" + "srcDc" +
                                "&mhaName=" + "mhaName" +
                                "&namespace=" + "；" +
                                "&name=" + "name" +
                                "&column=" + "columnA")
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
    }


    @Test
    public void testGetRealExecutedGtid()  {
        try(MockedStatic<MySqlUtils> theMock = Mockito.mockStatic(MySqlUtils.class)) {
            MySqlEndpoint mySqlEndpoint = new MySqlEndpoint("ip", 3306, "usr", "psw", true);
            theMock.when(() -> MySqlUtils.getUnionExecutedGtid(Mockito.any())).thenReturn("GtidSetString");
            MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/local/gtid?" +
                                    "mha=" + "mha1" +
                                    "&ip=" + "ip1" +
                                    "&port=" + 3306 +
                                    "&user=" + "usr" +
                                    "&psw=" + "psw")
                            .accept(MediaType.APPLICATION_JSON))
                    .andDo(MockMvcResultHandlers.print())
                    .andReturn();
            int status = mvcResult.getResponse().getStatus();
            String response = mvcResult.getResponse().getContentAsString();
            Assert.assertEquals(200, status);
            System.out.println(response);

            theMock.when(() -> MySqlUtils.getUnionExecutedGtid(Mockito.any())).
                    thenReturn(null);
             mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/local/gtid?" +
                                    "mha=" + "mha1" +
                                    "&ip=" + "ip1" +
                                    "&port=" + 3306 +
                                    "&user=" + "usr" +
                                    "&psw=" + "psw")
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


    @Test
    public void testPreCheckConfig() throws Exception {
        HashMap<String, Object> res = Maps.newHashMap();
        res.put("binlogMode", "ON");
        res.put("binlogFormat", "ROW");
        res.put("binlogVersion1", "OFF");
        res.put("binlogTransactionDependency", "WRITESET");
        res.put("gtidMode", "ON");
        res.put("drcTables", 2);
        res.put("autoIncrementStep", 2);
        res.put("autoIncrementOffset", 1);
        res.put("drcAccounts", "three accounts ready");
        Mockito.when(localService.preCheckMySqlConfig(Mockito.eq("mha1"))).thenReturn(res);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/local/preCheckMySqlConfig?" +
                                "mha=" + "mha1")
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);

        Mockito.when(localService.preCheckMySqlConfig(Mockito.eq("mha1"))).thenThrow(new RuntimeException("test"));
        mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/local/preCheckMySqlConfig?" +
                                "mha=" + "mha1")
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
    }

    @Test
    public void testPreCheckTables() throws Exception {
        List<TableCheckVo> checkVos = org.assertj.core.util.Lists.newArrayList();
        TableCheckVo tableCheckVo = new TableCheckVo(new MySqlUtils.TableSchemaName("schema1", "table1"));
        checkVos.add(tableCheckVo);
        System.out.println(tableCheckVo.toString());
        Mockito.when(localService.preCheckMySqlTables(Mockito.eq("mha1"),Mockito.eq(",*"))).thenReturn(checkVos);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/local/preCheckMySqlTables?" +
                                "mha=" + "mha1" +
                                "&nameFilter=" + ".*")
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);

        Mockito.when(localService.preCheckMySqlTables(Mockito.eq("mha1"),Mockito.eq(",*"))).thenThrow(new RuntimeException("test"));
        mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/local/preCheckMySqlTables?" +
                                "mha=" + "mha1" +
                                "&nameFilter=" + ".*")
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
    }
}