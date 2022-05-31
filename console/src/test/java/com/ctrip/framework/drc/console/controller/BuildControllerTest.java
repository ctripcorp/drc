package com.ctrip.framework.drc.console.controller;


import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.ApplierGroupTblDao;
import com.ctrip.framework.drc.console.dao.ReplicatorGroupTblDao;
import com.ctrip.framework.drc.console.dto.RowsFilterConfigDto;
import com.ctrip.framework.drc.console.service.RowsFilterService;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.console.utils.DalUtils;
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
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.sql.SQLException;


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
    private ReplicatorGroupTblDao replicatorGroupTblDao ;
    
    @Mock
    private ApplierGroupTblDao applierGroupTblDao;
    
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
}