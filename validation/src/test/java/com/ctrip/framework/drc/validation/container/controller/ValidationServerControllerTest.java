package com.ctrip.framework.drc.validation.container.controller;

import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.validation.AllTests;
import com.ctrip.framework.drc.validation.container.ValidationServerContainer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

public class ValidationServerControllerTest {

    @InjectMocks
    private ValidationServerController controller;

    @Mock
    private ValidationServerContainer serverContainer;

    private ObjectMapper objectMapper = new ObjectMapper();

    private MockMvc mvc;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        mvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    public void testAdd() throws Exception {
        Mockito.doReturn(true).when(serverContainer).addServer(Mockito.any());
        ApiResult result = controller.put(AllTests.getValidationConfigDto());
        Assert.assertEquals("true", result.getData().toString());

        Mockito.doThrow(new Exception()).when(serverContainer).addServer(Mockito.any());
        result = controller.put(AllTests.getValidationConfigDto());
        Assert.assertEquals("false", result.getData().toString());
    }

    @Test
    public void testRemove() throws Exception {
        Mockito.doNothing().when(serverContainer).removeServer(Mockito.any());
        ApiResult result = controller.remove("");
        Assert.assertEquals("true", result.getData().toString());

        Mockito.doThrow(new Exception()).when(serverContainer).removeServer(Mockito.any());
        result = controller.remove("");
        Assert.assertEquals("false", result.getData().toString());
    }
}
