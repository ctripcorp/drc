package com.ctrip.framework.drc.console.controller;


import com.ctrip.framework.drc.console.dto.ColumnsFilterConfigDto;
import com.ctrip.framework.drc.console.dto.DataMediaDto;
import com.ctrip.framework.drc.console.service.DataMediaService;
import java.sql.SQLException;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

public class DataMediaControllerTest extends AbstractControllerTest{
    
    @InjectMocks private DataMediaController dataMediaController;
    
    @Mock  private DataMediaService dataMediaService;

    @Before
    public void setUp() throws Exception {
        /** initialization */
        MockitoAnnotations.openMocks(this);
        /** build mvc env */
        mvc = MockMvcBuilders.standaloneSetup(dataMediaController).build();
    }

    @Test
    public void testInputDataMediaConfig() throws Exception {
        DataMediaDto dto = new DataMediaDto();
        dto.setId(1L);
        dto.setNamespace("db1");
        dto.setName("table1");
        dto.setApplierGroupId(1L);
        dto.setType(0);
        dto.setDataMediaSourceId(1L);
        Mockito.when(dataMediaService.processUpdateDataMedia(Mockito.any(DataMediaDto.class))).thenReturn(null);
        MvcResult mvcResult = doNormalPost("/api/drc/v1/dataMedia/dataMediaConfig",dto);
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);

        dto.setId(0L);
        Mockito.when(dataMediaService.processAddDataMedia(Mockito.any(DataMediaDto.class))).thenReturn(null);
        mvcResult = doNormalPost("/api/drc/v1/dataMedia/dataMediaConfig",dto);
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);

        Mockito.when(dataMediaService.processAddDataMedia(Mockito.any(DataMediaDto.class))).thenThrow(new SQLException());
        mvcResult = doNormalPost("/api/drc/v1/dataMedia/dataMediaConfig",dto);
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
    }

    @Test
    public void testDeleteDataMediaConfig() throws Exception {
        Mockito.when(dataMediaService.processDeleteDataMedia(Mockito.anyLong())).thenReturn(null);
        MvcResult mvcResult = doNormalDelete("/api/drc/v1/dataMedia/dataMediaConfig/1");
        
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);

        Mockito.when(dataMediaService.processDeleteDataMedia(Mockito.anyLong())).thenThrow(new SQLException());
        mvcResult = doNormalDelete("/api/drc/v1/dataMedia/dataMediaConfig/1");
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
    }

    @Test
    public void testInputColumnsFilterConfig() throws Exception {
        ColumnsFilterConfigDto dto = new ColumnsFilterConfigDto();
        dto.setId(1L);
        dto.setDataMediaId(1L);
        dto.setMode("exclude");
        dto.setColumns(Lists.newArrayList("column1"));
        Mockito.when(dataMediaService.processUpdateColumnsFilterConfig(Mockito.any(ColumnsFilterConfigDto.class))).thenReturn(null);
        MvcResult mvcResult = doNormalPost("/api/drc/v1/dataMedia/columnsFilterConfig",dto);
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);

        dto.setId(0L);
        Mockito.when(dataMediaService.processAddColumnsFilterConfig(Mockito.any(ColumnsFilterConfigDto.class))).thenReturn(null);
        mvcResult = doNormalPost("/api/drc/v1/dataMedia/columnsFilterConfig",dto);
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);

        Mockito.when(dataMediaService.processAddColumnsFilterConfig(Mockito.any(ColumnsFilterConfigDto.class))).thenThrow(new SQLException());
        mvcResult = doNormalPost("/api/drc/v1/dataMedia/columnsFilterConfig",dto);
        
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
    }

    @Test
    public void testDeleteColumnsFilterConfig() throws Exception {
        Mockito.when(dataMediaService.processDeleteColumnsFilterConfig(Mockito.anyLong())).thenReturn(null);
        MvcResult mvcResult = doNormalDelete("/api/drc/v1/dataMedia/columnsFilterConfig/1");
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);

        Mockito.when(dataMediaService.processDeleteColumnsFilterConfig(Mockito.anyLong())).thenThrow(new SQLException());
        mvcResult = doNormalDelete("/api/drc/v1/dataMedia/columnsFilterConfig/1");
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
    }
}