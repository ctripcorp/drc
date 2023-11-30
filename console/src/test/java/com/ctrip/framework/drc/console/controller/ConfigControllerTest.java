package com.ctrip.framework.drc.console.controller;

import static org.mockito.Mockito.when;

import com.ctrip.framework.drc.console.controller.v1.ConfigController;
import com.ctrip.framework.drc.console.service.impl.ConfigServiceImpl;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-01-07
 */
public class ConfigControllerTest {

    private MockMvc mvc;

    @InjectMocks
    private ConfigController controller;

    @Mock
    private ConfigServiceImpl configService;



    private ObjectMapper objectMapper = new ObjectMapper();

    @Before
    public void setUp() {
        String[] dataTypes = {"int", "char", "boolean"};

        /** initialization */
        MockitoAnnotations.initMocks(this);
        /** build mvc env */
        mvc = MockMvcBuilders.standaloneSetup(controller).build();

        when(configService.getAllDrcSupportDataTypes()).thenReturn(dataTypes);
    }

    @Test
    public void testGetAllDrcSupportDataType() throws Exception {

        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/data/types").accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        Assert.assertNotNull(response);
        Assert.assertNotEquals("", response);
    }

    @Test
    public void testGetHealthStatus() throws Exception {
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/health").accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        String response = mvcResult.getResponse().getContentAsString();
        Assert.assertEquals(200, status);
        System.out.println(response);
        Assert.assertNotNull(response);
        Assert.assertNotEquals("", response);
    }

}
