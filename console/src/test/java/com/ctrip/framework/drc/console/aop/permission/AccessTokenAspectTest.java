package com.ctrip.framework.drc.console.aop.permission;


import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.controller.log.ConflictLogController;
import com.ctrip.framework.drc.console.enums.TokenType;
import com.ctrip.framework.drc.console.enums.log.CflBlacklistType;
import com.ctrip.framework.drc.console.service.log.ConflictLogService;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.util.DigestUtils;

import java.sql.SQLException;

public class AccessTokenAspectTest {
    
    @InjectMocks
    private AccessTokenAspect aop;
    
    @Mock
    private DefaultConsoleConfig consoleConfig;
    
    @InjectMocks
    private ConflictLogController conflictLogController;
    
    @Mock
    private ConflictLogService conflictLogService;
    
    private ConflictLogController proxy;

    private MockMvc mvc;

    protected ObjectMapper objectMapper = new ObjectMapper();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        Mockito.when(consoleConfig.getDrcAccessTokenKey()).thenReturn("mockTokenKey");
        AspectJProxyFactory factory = new AspectJProxyFactory(conflictLogController);
        factory.setProxyTargetClass(true);
        factory.addAspect(aop);
        proxy = factory.getProxy();
        mvc = MockMvcBuilders.standaloneSetup(proxy).build();
    }
    
    @Test
    public void testAccessTokenCheck() throws Exception {
        try {
            Mockito.doNothing().when(conflictLogService).addDbBlacklist(Mockito.anyString(),Mockito.any(
                    CflBlacklistType.class),Mockito.any());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v2/log/conflict/blacklist/dba/touchjob?db=db1&table=table1")
                        .header("DRC-Access-Token", "a2cf09da41f37ea6ec7be2b9a3d650b2")
                        .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        assertNormalResponse(mvcResult);


        mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v2/log/conflict/blacklist/dba/touchjob?db=db1&table=table1")
                        .header("DRC-Access-Token", "ss")
                        .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        assertFailResponse(mvcResult);
    }
    
    @Test
    public void testMD5() {
        StringBuilder stringBuilder = new StringBuilder("0d7ee9a0-229c-4481-9733-80ab717f120b");
        stringBuilder.append("_").append(TokenType.OPEN_API_4_DBA.getCode());
        //a2cf09da41f37ea6ec7be2b9a3d650b2
        System.out.println(DigestUtils.md5DigestAsHex(stringBuilder.toString().getBytes()));
    }
    
    @Test
    public void  getUUID() {
        System.out.println(java.util.UUID.randomUUID().toString());
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

    protected void assertFailResponse(MvcResult mvcResult) throws Exception {
        MockHttpServletResponse response = mvcResult.getResponse();
        response.setContentType("application/json");
        int status = response.getStatus();
        String responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        ApiResult result = objectMapper.readValue(responseStr, ApiResult.class);
        Assert.assertEquals(1, result.getStatus().intValue());
        Assert.assertEquals("DRC-Access-Token is invalid!",result.getMessage());
    }
}