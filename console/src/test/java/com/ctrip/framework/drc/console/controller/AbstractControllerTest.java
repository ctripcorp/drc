package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.AbstractTest;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-08-11
 */
public abstract class AbstractControllerTest extends AbstractTest {
    protected MockMvc mvc;

    protected ObjectMapper objectMapper = new ObjectMapper();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.initMocks(this);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    protected MvcResult doNormalGet(String uri) throws Exception {
        return mvc.perform(MockMvcRequestBuilders.get(uri).accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
    }

    protected MvcResult doNormalPost(String uri) throws Exception {
        return mvc.perform(MockMvcRequestBuilders.post(uri))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
    }

    protected MvcResult doNormalPost(String uri, Object obj) throws Exception {
        return mvc.perform(MockMvcRequestBuilders.post(uri)
                .contentType(MediaType.APPLICATION_JSON).content(getRequestBody(obj))
                .accept(MediaType.APPLICATION_JSON))
                .andReturn();
    }

    protected MvcResult doNormalPut(String uri) throws Exception {
        return mvc.perform(MockMvcRequestBuilders.put(uri))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
    }

    protected MvcResult doNormalPut(String uri, Object obj) throws Exception {
        return mvc.perform(MockMvcRequestBuilders.put(uri)
                .contentType(MediaType.APPLICATION_JSON).content(getRequestBody(obj))
                .accept(MediaType.APPLICATION_JSON))
                .andReturn();
    }

    protected MvcResult doNormalDelete(String uri) throws Exception {
        return mvc.perform(MockMvcRequestBuilders.delete(uri).accept(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();
    }

    protected MvcResult doNormalDelete(String uri, Object obj) throws Exception {
        return mvc.perform(MockMvcRequestBuilders.delete(uri)
                .contentType(MediaType.APPLICATION_JSON).content(getRequestBody(obj))
                .accept(MediaType.APPLICATION_JSON))
                .andReturn();
    }

    protected void assertNormalResponse(MvcResult mvcResult) throws Exception {
        MockHttpServletResponse response = mvcResult.getResponse();
        response.setContentType("application/json");
        int status = response.getStatus();
        String responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        ApiResult result = objectMapper.readValue(responseStr, ApiResult.class);
        Assert.assertEquals(0, result.getStatus().intValue());
        Assert.assertTrue((boolean)result.getData());
    }

    protected void assertNormalResponseWithoutCheckingData(MvcResult mvcResult, ResultCode resultCode) throws Exception {
        MockHttpServletResponse response = mvcResult.getResponse();
        int status = response.getStatus();
        response.setContentType("application/json");
        String responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        ApiResult result = objectMapper.readValue(responseStr, ApiResult.class);
        Assert.assertEquals(resultCode.getCode(), result.getStatus().intValue());
        System.out.println(result.getData());
    }

    protected String getRequestBody(Object dto) throws JsonProcessingException {
        return objectMapper.writeValueAsString(dto);
    }
}
