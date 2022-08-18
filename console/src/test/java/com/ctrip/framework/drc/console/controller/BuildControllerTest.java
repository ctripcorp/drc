package com.ctrip.framework.drc.console.controller;


import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.ApplierGroupTblDao;
import com.ctrip.framework.drc.console.dao.ReplicatorGroupTblDao;
import com.ctrip.framework.drc.console.dto.RowsFilterConfigDto;
import com.ctrip.framework.drc.console.service.DrcBuildService;
import com.ctrip.framework.drc.console.service.RowsFilterService;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.TableCheckVo;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.server.common.filter.row.FetchMode;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.assertj.core.util.Lists;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;


public class BuildControllerTest extends AbstractControllerTest {
    
    @InjectMocks
    private BuildController buildController;
    
    @Mock
    private MetaInfoServiceImpl metaInfoService;

    @Mock
    private RowsFilterService rowsFilterService;

    @Mock
    private DefaultConsoleConfig consoleConfig;
    
    @Mock
    private DalUtils dalUtils;
    
    @Mock
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    
    @Mock
    private ApplierGroupTblDao applierGroupTblDao;
    
    @Mock
    private DrcBuildService drcBuildService;
    
    @Before
    public void setUp() throws SQLException {
        /** initialization */
        MockitoAnnotations.openMocks(this);
        /** build mvc env */
        mvc = MockMvcBuilders.standaloneSetup(buildController).build();
    }
    
    @Test
    public void testGetOrBuildSimplexDrc() throws Exception {
        Mockito.when(dalUtils.getId(Mockito.any(),Mockito.eq("srcMha"))).thenReturn(1L);
        Mockito.when(dalUtils.getId(Mockito.any(),Mockito.eq("destMha"))).thenReturn(2L);
        Mockito.when(metaInfoService.getDc(Mockito.eq("srcMha"))).thenReturn("dc1");
        Mockito.when(metaInfoService.getDc(Mockito.eq("destMha"))).thenReturn("dc2");
        Mockito.when(replicatorGroupTblDao.upsertIfNotExist(Mockito.eq(1L))).thenReturn(1L);
        Mockito.when(applierGroupTblDao.upsertIfNotExist(Mockito.eq(1L),Mockito.eq(2L))).thenReturn(1L);

        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/build/simplexDrc/srcMha/destMha")
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
        Assert.assertNotNull(response);

        Mockito.when(dalUtils.getId(Mockito.any(),Mockito.eq("srcMha"))).thenThrow(new SQLException());
        mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/build/simplexDrc/srcMha/destMha")
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
        Assert.assertNotNull(response);
    }

    @Test
    public void testGetRowsFilterMappingVos() throws Exception {
        Mockito.when(rowsFilterService.getRowsFilterMappingVos(Mockito.anyLong())).thenReturn(null);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/build/rowsFilterMappings/1")
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);

        Mockito.when(rowsFilterService.getRowsFilterMappingVos(Mockito.anyLong())).thenThrow(new SQLException());
        mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/build/rowsFilterMappings/1")
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
    }

    @Test
    public void testInputRowsFilter() throws Exception {
        RowsFilterConfigDto dto = new RowsFilterConfigDto();
        dto.setId(1L);
        dto.setContext("context");
        dto.setFetchMode(FetchMode.RPC.getCode());
        Mockito.when(rowsFilterService.updateRowsFilterConfig(Mockito.any(RowsFilterConfigDto.class))).thenReturn(null);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/build/rowsFilterConfig").
                contentType(MediaType.APPLICATION_JSON).content(getRequestBody(dto)).
                accept(MediaType.APPLICATION_JSON)).andDo(MockMvcResultHandlers.print()).andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);

        dto.setId(null);
        Mockito.when(rowsFilterService.addRowsFilterConfig(Mockito.any(RowsFilterConfigDto.class))).thenReturn(null);
        mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/build/rowsFilterConfig").
                contentType(MediaType.APPLICATION_JSON).content(getRequestBody(dto)).
                accept(MediaType.APPLICATION_JSON)).andDo(MockMvcResultHandlers.print()).andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);

        Mockito.when(rowsFilterService.updateRowsFilterConfig(Mockito.any(RowsFilterConfigDto.class))).thenThrow(new SQLException());
        mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/build/rowsFilterConfig").
                contentType(MediaType.APPLICATION_JSON).content(getRequestBody(dto)).
                accept(MediaType.APPLICATION_JSON)).andDo(MockMvcResultHandlers.print()).andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
    }

    @Test
    public void testDeleteRowsFilterConfig() throws Exception {
        Mockito.when(rowsFilterService.deleteRowsFilterConfig(Mockito.anyLong())).thenReturn(null);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.delete("/api/drc/v1/build/rowsFilterConfig/1")
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);

        Mockito.when(rowsFilterService.deleteRowsFilterConfig(Mockito.anyLong())).thenThrow(new SQLException());
        mvcResult = mvc.perform(MockMvcRequestBuilders.delete("/api/drc/v1/build/rowsFilterConfig/1")
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
    }

    @Test
    public void testGetConflictTables() throws Exception {
        Mockito.when(rowsFilterService.getLogicalTables(
                Mockito.anyLong(),
                Mockito.anyLong(),
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyString())).thenReturn(Lists.newArrayList("db2\\..*","db1\\.*"));
        Mockito.when(rowsFilterService.getConflictTables(Mockito.anyString(),Mockito.anyString())).
                thenReturn(Lists.newArrayList("conflictTable1"));
        Map<String, String> consoleDcInfos = Maps.newHashMap();
        HashSet<String> publicDc = Sets.newHashSet("publicDc");
        consoleDcInfos.put("publicDc","domain");
        Mockito.when(consoleConfig.getConsoleDcInfos()).thenReturn(consoleDcInfos);
        Mockito.when(consoleConfig.getPublicCloudDc()).thenReturn(publicDc);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/build/dataMedia/conflictCheck?" +
                                "applierGroupId=" + 0L +
                                "&dataMediaId=" + 0L +
                                "&srcDc=" + "notPulicDc" +
                                "&mhaName=" + "mha1" +
                                "&namespace=" + "db1" +
                                "&name=" + ".*" )
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);

        try(MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when(() -> HttpUtils.get(Mockito.anyString())).thenReturn(ApiResult.getSuccessInstance(null));
             mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/build/dataMedia/conflictCheck?" +
                                    "applierGroupId=" + 0L +
                                    "&dataMediaId=" + 0L +
                                    "&srcDc=" + "publicDc" +
                                    "&mhaName=" + "mha1" +
                                    "&namespace=" + "db1" +
                                    "&name=" + ".*" )
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
        Mockito.when(drcBuildService.preCheckMySqlConfig(Mockito.eq("mha1"))).thenReturn(res);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/build/preCheckMySqlConfig?" +
                                "mha=" + "mha1")
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);

        Mockito.when(drcBuildService.preCheckMySqlConfig(Mockito.eq("mha1"))).thenThrow(new RuntimeException("test"));
         mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/build/preCheckMySqlConfig?" +
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
        List<TableCheckVo> checkVos = Lists.newArrayList();
        TableCheckVo tableCheckVo = new TableCheckVo(new MySqlUtils.TableSchemaName("schema1", "table1"));
        checkVos.add(tableCheckVo);
        System.out.println(tableCheckVo.toString());
        Mockito.when(drcBuildService.preCheckMySqlTables(Mockito.eq("mha1"),Mockito.eq(",*"))).thenReturn(checkVos);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/build/preCheckMySqlTables?" +
                                "mha=" + "mha1" +
                                "&nameFilter=" + ".*")
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);

        Mockito.when(drcBuildService.preCheckMySqlTables(Mockito.eq("mha1"),Mockito.eq(",*"))).thenThrow(new RuntimeException("test"));
        mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/build/preCheckMySqlTables?" +
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