package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.service.OpenApiService;
import org.assertj.core.util.Lists;
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

import java.sql.SQLException;

import static org.junit.Assert.*;

public class OpenApiControllerTest {
    
    private MockMvc mvc;
    
    @InjectMocks
    private OpenApiController openApiController;
    
    @Mock
    private OpenApiService openApiService;
    
    @Before
    public void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        mvc = MockMvcBuilders.standaloneSetup(openApiController).build();
        Mockito.doReturn(Lists.newArrayList()).when(openApiService).getAllDrcMhaDbFilters();
    }

    @Test
    public void testGetDrcAllMhaDbFiltersInfo() throws Exception {
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/openapi/info/mhas")
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
}