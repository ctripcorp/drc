package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.dto.MqConfigDto;
import com.ctrip.framework.drc.console.dto.RowsFilterConfigDto;
import com.ctrip.framework.drc.console.service.MessengerService;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.sql.SQLException;

import static org.junit.Assert.*;

public class MessengerControllerTest extends AbstractControllerTest {
    
    @InjectMocks MessengerController messengerController;

    @Mock MessengerService messengerService;
    
    @Before
    public void setUp() throws Exception {
        /** initialization */
        MockitoAnnotations.openMocks(this);
        /** build mvc env */
        mvc = MockMvcBuilders.standaloneSetup(messengerController).build();
    }

    @Test
    public void testGetAllMessengerVos() throws Exception {
        Mockito.when(messengerService.getAllMessengerVos()).thenReturn(Lists.newArrayList());
        
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/messenger/all")
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
        Assert.assertNotNull(response);

        Mockito.when(messengerService.getAllMessengerVos()).thenThrow(new SQLException());
        mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/messenger/all")
                        .accept(MediaType.APPLICATION_JSON)).andDo(MockMvcResultHandlers.print()).andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);

    }

    @Test
    public void testRemoveMessengerGroupInMha() throws Exception {
        Mockito.when(messengerService.removeMessengerGroup(Mockito.anyString())).thenReturn("remove success");

        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.delete("/api/drc/v1/messenger/?mhaName=mha1")
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
        Assert.assertNotNull(response);

        Mockito.when(messengerService.removeMessengerGroup(Mockito.anyString())).thenThrow(new SQLException());
        mvcResult = mvc.perform(MockMvcRequestBuilders.delete("/api/drc/v1/messenger/?mhaName=mha1")
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
    }

    @Test
    public void testGetAllMqConfigsByMessengerGroupId() throws Exception {
        Mockito.when(messengerService.getMqConfigVos(Mockito.anyLong())).thenReturn(Lists.newArrayList());

        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/messenger/mqConfigs/1")
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
        Assert.assertNotNull(response);

        Mockito.when(messengerService.getMqConfigVos(Mockito.anyLong())).thenThrow(new SQLException());
        mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/messenger/mqConfigs/1")
                .accept(MediaType.APPLICATION_JSON)).andDo(MockMvcResultHandlers.print()).andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
    }

    @Test
    public void testInputMqConfig() throws Exception {
        MqConfigDto mqConfigDto = new MqConfigDto();
        mqConfigDto.setId(1L);
        
        Mockito.when(messengerService.processUpdateMqConfig(Mockito.any(MqConfigDto.class))).thenReturn("updateMqConfig success");
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/messenger/mqConfig")
                .contentType(MediaType.APPLICATION_JSON).content(getRequestBody(mqConfigDto)).
                        accept(MediaType.APPLICATION_JSON)).andDo(MockMvcResultHandlers.print()).andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
        Assert.assertNotNull(response);


        mqConfigDto.setId(0L);
        Mockito.when(messengerService.processAddMqConfig(Mockito.any(MqConfigDto.class))).thenReturn("addMqConfig success");
        mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/messenger/mqConfig")
                .contentType(MediaType.APPLICATION_JSON).content(getRequestBody(mqConfigDto)).
                accept(MediaType.APPLICATION_JSON)).andDo(MockMvcResultHandlers.print()).andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
        Assert.assertNotNull(response);
        
        
        Mockito.when(messengerService.processAddMqConfig(Mockito.any(MqConfigDto.class))).thenThrow(new SQLException());
        mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/messenger/mqConfig")
                .contentType(MediaType.APPLICATION_JSON).content(getRequestBody(mqConfigDto)).
                accept(MediaType.APPLICATION_JSON)).andDo(MockMvcResultHandlers.print()).andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
        Assert.assertNotNull(response);
    }

    @Test
    public void testDeleteMqConfig() throws Exception {
        Mockito.when(messengerService.processDeleteMqConfig(Mockito.anyLong())).thenReturn("deleteMqConfig success");

        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.delete("/api/drc/v1/messenger/mqConfig/1")
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
        Assert.assertNotNull(response);

        mvcResult = mvc.perform(MockMvcRequestBuilders.delete("/api/drc/v1/messenger/mqConfig/0")
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);

        Mockito.when(messengerService.processDeleteMqConfig(Mockito.anyLong())).thenThrow(new SQLException());
        mvcResult = mvc.perform(MockMvcRequestBuilders.delete("/api/drc/v1/messenger/mqConfig/1")
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
    }

    @Test
    public void testGetBuListFromQmq() throws Exception {
        Mockito.when(messengerService.getBusFromQmq()).thenReturn(null);
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/messenger/qmq/bus").
                contentType(MediaType.APPLICATION_JSON).
                accept(MediaType.APPLICATION_JSON)).
                andDo(MockMvcResultHandlers.print()).
                andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);

    

        Mockito.when(messengerService.getBusFromQmq()).thenThrow(new SQLException());
        mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/messenger/qmq/bus").
                contentType(MediaType.APPLICATION_JSON).
                accept(MediaType.APPLICATION_JSON)).
                andDo(MockMvcResultHandlers.print()).
                andReturn();
        status = mvcResult.getResponse().getStatus();
        response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
        
    }
}