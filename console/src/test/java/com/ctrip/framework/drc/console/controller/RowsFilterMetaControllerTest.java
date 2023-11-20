package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.controller.v1.RowsFilterMetaController;
import com.ctrip.framework.drc.console.param.filter.RowsFilterMetaMappingCreateParam;
import com.ctrip.framework.drc.console.param.filter.RowsFilterMetaMessageCreateParam;
import com.ctrip.framework.drc.console.param.filter.RowsMetaFilterParam;
import com.ctrip.framework.drc.console.service.filter.RowsFilterMetaMappingService;
import com.ctrip.framework.drc.console.service.filter.RowsFilterMetaService;
import com.ctrip.framework.drc.console.vo.filter.QConfigDataVO;
import com.ctrip.framework.drc.console.vo.filter.RowsFilterMetaMappingVO;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.gson.JsonObject;
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

/**
 * Created by dengquanliang
 * 2023/5/15 17:01
 */
public class RowsFilterMetaControllerTest {

    private MockMvc mvc;

    @InjectMocks
    private RowsFilterMetaController rowsFilterMetaController;
    @Mock
    private RowsFilterMetaMappingService rowsFilterMetaMappingService;
    @Mock
    private RowsFilterMetaService rowsFilterMetaService;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        mvc = MockMvcBuilders.standaloneSetup(rowsFilterMetaController).build();
    }

    @Test
    public void testGetWhitelist() throws Exception {
        Mockito.when(rowsFilterMetaService.getWhitelist(Mockito.anyString())).thenReturn(new QConfigDataVO());
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/filter/row/metaFilterName/metaFilterName")
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        JsonObject jsonObject = JsonUtils.fromJson(response, JsonObject.class);
        Assert.assertNotNull(jsonObject.get("data"));
        Assert.assertEquals(0, jsonObject.get("status").getAsInt());

        Mockito.when(rowsFilterMetaService.getWhitelist(Mockito.anyString())).thenThrow(new SQLException());
        mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/filter/row/metaFilterName/metaFilterName")
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        jsonObject = JsonUtils.fromJson(response, JsonObject.class);
        Assert.assertEquals("null", jsonObject.get("data").toString());
        Assert.assertEquals(1, jsonObject.get("status").getAsInt());

    }

    @Test
    public void testAddWhitelist() throws Exception {
        Mockito.when(rowsFilterMetaService.addWhitelist(Mockito.any(RowsMetaFilterParam.class), Mockito.anyString())).thenReturn(true);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.put("/api/drc/v1/filter/row?operator=operator").contentType(MediaType.APPLICATION_JSON_UTF8).content(getRowsMetaFilterParamString())
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        JsonObject jsonObject = JsonUtils.fromJson(response, JsonObject.class);
        Assert.assertEquals(true, jsonObject.get("data").getAsBoolean());
        Assert.assertEquals(0, jsonObject.get("status").getAsInt());

        Mockito.when(rowsFilterMetaService.addWhitelist(Mockito.any(RowsMetaFilterParam.class), Mockito.anyString())).thenReturn(false);
        mvcResult = mvc.perform(MockMvcRequestBuilders.put("/api/drc/v1/filter/row?operator=operator").contentType(MediaType.APPLICATION_JSON_UTF8).content(getRowsMetaFilterParamString())
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        jsonObject = JsonUtils.fromJson(response, JsonObject.class);
        Assert.assertEquals(false, jsonObject.get("data").getAsBoolean());
        Assert.assertEquals(1, jsonObject.get("status").getAsInt());

        Mockito.when(rowsFilterMetaService.addWhitelist(Mockito.any(RowsMetaFilterParam.class), Mockito.anyString())).thenThrow(new IllegalArgumentException());
        mvcResult = mvc.perform(MockMvcRequestBuilders.put("/api/drc/v1/filter/row?operator=operator").contentType(MediaType.APPLICATION_JSON_UTF8).content(getRowsMetaFilterParamString())
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        jsonObject = JsonUtils.fromJson(response, JsonObject.class);
        Assert.assertEquals(false, jsonObject.get("data").getAsBoolean());
        Assert.assertEquals(1, jsonObject.get("status").getAsInt());

    }

    @Test
    public void testDeleteWhitelist() throws Exception {
        Mockito.when(rowsFilterMetaService.deleteWhitelist(Mockito.any(RowsMetaFilterParam.class), Mockito.anyString())).thenReturn(true);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.delete("/api/drc/v1/filter/row?operator=operator").contentType(MediaType.APPLICATION_JSON_UTF8).content(getRowsMetaFilterParamString())
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        JsonObject jsonObject = JsonUtils.fromJson(response, JsonObject.class);
        Assert.assertEquals(true, jsonObject.get("data").getAsBoolean());
        Assert.assertEquals(0, jsonObject.get("status").getAsInt());

        Mockito.when(rowsFilterMetaService.deleteWhitelist(Mockito.any(RowsMetaFilterParam.class), Mockito.anyString())).thenReturn(false);
        mvcResult = mvc.perform(MockMvcRequestBuilders.delete("/api/drc/v1/filter/row?operator=operator").contentType(MediaType.APPLICATION_JSON_UTF8).content(getRowsMetaFilterParamString())
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        jsonObject = JsonUtils.fromJson(response, JsonObject.class);
        Assert.assertEquals(false, jsonObject.get("data").getAsBoolean());
        Assert.assertEquals(1, jsonObject.get("status").getAsInt());

        Mockito.when(rowsFilterMetaService.deleteWhitelist(Mockito.any(RowsMetaFilterParam.class), Mockito.anyString())).thenThrow(new IllegalArgumentException());
        mvcResult = mvc.perform(MockMvcRequestBuilders.delete("/api/drc/v1/filter/row?operator=operator").contentType(MediaType.APPLICATION_JSON_UTF8).content(getRowsMetaFilterParamString())
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        jsonObject = JsonUtils.fromJson(response, JsonObject.class);
        Assert.assertEquals(false, jsonObject.get("data").getAsBoolean());
        Assert.assertEquals(1, jsonObject.get("status").getAsInt());

    }

    @Test
    public void testUpdateWhitelist() throws Exception {
        Mockito.when(rowsFilterMetaService.updateWhitelist(Mockito.any(RowsMetaFilterParam.class), Mockito.anyString())).thenReturn(true);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/filter/row?operator=operator").contentType(MediaType.APPLICATION_JSON_UTF8).content(getRowsMetaFilterParamString())
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        JsonObject jsonObject = JsonUtils.fromJson(response, JsonObject.class);
        Assert.assertEquals(true, jsonObject.get("data").getAsBoolean());
        Assert.assertEquals(0, jsonObject.get("status").getAsInt());

        Mockito.when(rowsFilterMetaService.updateWhitelist(Mockito.any(RowsMetaFilterParam.class), Mockito.anyString())).thenReturn(false);
        mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/filter/row?operator=operator").contentType(MediaType.APPLICATION_JSON_UTF8).content(getRowsMetaFilterParamString())
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        jsonObject = JsonUtils.fromJson(response, JsonObject.class);
        Assert.assertEquals(false, jsonObject.get("data").getAsBoolean());
        Assert.assertEquals(1, jsonObject.get("status").getAsInt());

        Mockito.when(rowsFilterMetaService.updateWhitelist(Mockito.any(RowsMetaFilterParam.class), Mockito.anyString())).thenThrow(new IllegalArgumentException());
        mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/filter/row?operator=operator").contentType(MediaType.APPLICATION_JSON_UTF8).content(getRowsMetaFilterParamString())
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        jsonObject = JsonUtils.fromJson(response, JsonObject.class);
        Assert.assertEquals(false, jsonObject.get("data").getAsBoolean());
        Assert.assertEquals(1, jsonObject.get("status").getAsInt());

    }

    @Test
    public void testCreateMetaMessage() throws Exception {
        Mockito.when(rowsFilterMetaMappingService.createMetaMessage(Mockito.any(RowsFilterMetaMessageCreateParam.class))).thenReturn(true);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.put("/api/drc/v1/filter/row/meta").contentType(MediaType.APPLICATION_JSON_UTF8).content(getMetaMessageCreateParamString())
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        JsonObject jsonObject = JsonUtils.fromJson(response, JsonObject.class);
        Assert.assertEquals(true, jsonObject.get("data").getAsBoolean());
        Assert.assertEquals(0, jsonObject.get("status").getAsInt());

        Mockito.when(rowsFilterMetaMappingService.createMetaMessage(Mockito.any(RowsFilterMetaMessageCreateParam.class))).thenReturn(false);
        mvcResult = mvc.perform(MockMvcRequestBuilders.put("/api/drc/v1/filter/row/meta").contentType(MediaType.APPLICATION_JSON_UTF8).content(getMetaMessageCreateParamString())
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        jsonObject = JsonUtils.fromJson(response, JsonObject.class);
        Assert.assertEquals(false, jsonObject.get("data").getAsBoolean());
        Assert.assertEquals(1, jsonObject.get("status").getAsInt());

        Mockito.when(rowsFilterMetaMappingService.createMetaMessage(Mockito.any(RowsFilterMetaMessageCreateParam.class))).thenThrow(new IllegalStateException());
        mvcResult = mvc.perform(MockMvcRequestBuilders.put("/api/drc/v1/filter/row/meta").contentType(MediaType.APPLICATION_JSON_UTF8).content(getMetaMessageCreateParamString())
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        jsonObject = JsonUtils.fromJson(response, JsonObject.class);
        Assert.assertEquals(false, jsonObject.get("data").getAsBoolean());
        Assert.assertEquals(1, jsonObject.get("status").getAsInt());

    }

    @Test
    public void testCreateOrUpdateMetaMapping() throws Exception {
        Mockito.when(rowsFilterMetaMappingService.createOrUpdateMetaMapping(Mockito.any(RowsFilterMetaMappingCreateParam.class))).thenReturn(true);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/filter/row/mapping").contentType(MediaType.APPLICATION_JSON_UTF8).content(getMetaMappingCreateParamString())
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        JsonObject jsonObject = JsonUtils.fromJson(response, JsonObject.class);
        Assert.assertEquals(true, jsonObject.get("data").getAsBoolean());
        Assert.assertEquals(0, jsonObject.get("status").getAsInt());

        Mockito.when(rowsFilterMetaMappingService.createOrUpdateMetaMapping(Mockito.any(RowsFilterMetaMappingCreateParam.class))).thenReturn(false);
        mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/filter/row/mapping").contentType(MediaType.APPLICATION_JSON_UTF8).content(getMetaMappingCreateParamString())
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        jsonObject = JsonUtils.fromJson(response, JsonObject.class);
        Assert.assertEquals(false, jsonObject.get("data").getAsBoolean());
        Assert.assertEquals(1, jsonObject.get("status").getAsInt());

        Mockito.when(rowsFilterMetaMappingService.createOrUpdateMetaMapping(Mockito.any(RowsFilterMetaMappingCreateParam.class))).thenThrow(new IllegalStateException());
        mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/filter/row/mapping").contentType(MediaType.APPLICATION_JSON_UTF8).content(getMetaMappingCreateParamString())
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        jsonObject = JsonUtils.fromJson(response, JsonObject.class);
        Assert.assertEquals(false, jsonObject.get("data").getAsBoolean());
        Assert.assertEquals(1, jsonObject.get("status").getAsInt());

    }

    @Test
    public void testGetMetaMessageList() throws Exception {
        Mockito.when(rowsFilterMetaMappingService.getMetaMessages(Mockito.anyString(), Mockito.anyString())).thenReturn(new ArrayList<>());
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/filter/row/meta/all?metaFilterName=name&mhaName=mhaName")
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        JsonObject jsonObject = JsonUtils.fromJson(response, JsonObject.class);
        Assert.assertNotNull( jsonObject.get("data"));
        Assert.assertEquals(0, jsonObject.get("status").getAsInt());


        Mockito.when(rowsFilterMetaMappingService.getMetaMessages(Mockito.anyString(), Mockito.anyString())).thenThrow(new IllegalArgumentException());
        mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/filter/row/meta/all?metaFilterName=name&mhaName=mhaName")
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        jsonObject = JsonUtils.fromJson(response, JsonObject.class);
        Assert.assertEquals("null", jsonObject.get("data").toString());
        Assert.assertEquals(1, jsonObject.get("status").getAsInt());

    }

    @Test
    public void testGetMappingList() throws Exception {
        Mockito.when(rowsFilterMetaMappingService.getMetaMappings(Mockito.anyLong())).thenReturn(new RowsFilterMetaMappingVO());
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/filter/row/mapping?metaFilterId=1")
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        JsonObject jsonObject = JsonUtils.fromJson(response, JsonObject.class);
        Assert.assertNotNull( jsonObject.get("data"));
        Assert.assertEquals(0, jsonObject.get("status").getAsInt());


        Mockito.when(rowsFilterMetaMappingService.getMetaMappings(Mockito.anyLong())).thenThrow(new IllegalArgumentException());
        mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/filter/row/mapping?metaFilterId=1")
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        jsonObject = JsonUtils.fromJson(response, JsonObject.class);
        Assert.assertEquals("null", jsonObject.get("data").toString());
        Assert.assertEquals(1, jsonObject.get("status").getAsInt());

    }

    @Test
    public void testDeleteMetaMessage() throws Exception {
        Mockito.when(rowsFilterMetaMappingService.deleteMetaMessage(Mockito.anyLong())).thenReturn(true);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.delete("/api/drc/v1/filter/row/meta?metaFilterId=1")
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        JsonObject jsonObject = JsonUtils.fromJson(response, JsonObject.class);
        Assert.assertEquals(true, jsonObject.get("data").getAsBoolean());
        Assert.assertEquals(0, jsonObject.get("status").getAsInt());

        Mockito.when(rowsFilterMetaMappingService.deleteMetaMessage(Mockito.anyLong())).thenReturn(false);
        mvcResult = mvc.perform(MockMvcRequestBuilders.delete("/api/drc/v1/filter/row/meta?metaFilterId=1")
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        jsonObject = JsonUtils.fromJson(response, JsonObject.class);
        Assert.assertEquals(false, jsonObject.get("data").getAsBoolean());
        Assert.assertEquals(1, jsonObject.get("status").getAsInt());

        Mockito.when(rowsFilterMetaMappingService.deleteMetaMessage(Mockito.anyLong())).thenThrow(new IllegalArgumentException());
        mvcResult = mvc.perform(MockMvcRequestBuilders.delete("/api/drc/v1/filter/row/meta?metaFilterId=1")
                .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        jsonObject = JsonUtils.fromJson(response, JsonObject.class);
        Assert.assertEquals(false, jsonObject.get("data").getAsBoolean());
        Assert.assertEquals(1, jsonObject.get("status").getAsInt());

    }

    private String getMetaMessageCreateParamString() {
        return "{\"metaFilterName\": \"name\", \"targetSubEnv\":[], \"bu\":\"bu\", \"owner\":\"owner\", \"filterType\": 2}";
    }

    private String getMetaMappingCreateParamString() {
        return "{\"metaFilterId\": 1, \"filterKeys\":[]}";
    }
    
    private String getRowsMetaFilterParamString() {
        return "{\"metaFilterName\": \"data\", \"whitelist\":[]}";
    }


}
