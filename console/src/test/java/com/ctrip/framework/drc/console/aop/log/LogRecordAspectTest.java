package com.ctrip.framework.drc.console.aop.log;

import static org.mockito.Mockito.*;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.controller.log.ConflictLogController;
import com.ctrip.framework.drc.console.dao.log.entity.OperationLogTbl;
import com.ctrip.framework.drc.console.param.log.ConflictTrxLogQueryParam;
import com.ctrip.framework.drc.console.service.log.ConflictLogService;
import com.ctrip.framework.drc.console.service.log.LogRecordService;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.service.user.UserService;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.SQLException;
import org.assertj.core.util.Lists;
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

public class LogRecordAspectTest {

    @InjectMocks
    private LogRecordAspect aop;

    @Mock
    private DefaultConsoleConfig consoleConfig;
    
    @Mock
    private LogRecordService logRecordService;
    
    @Mock
    private UserService userService;

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
       
        
        AspectJProxyFactory factory = new AspectJProxyFactory(conflictLogController);
        factory.setProxyTargetClass(true);
        factory.addAspect(aop);
        proxy = factory.getProxy();
        mvc = MockMvcBuilders.standaloneSetup(proxy).build();
    }
    
    @Test
    public void testRecord() throws Exception {
        when(consoleConfig.getOperationLogSwitch()).thenReturn(true);
        when(userService.getInfo()).thenReturn("userName");
        
        // success
        doNothing().when(logRecordService).record(Mockito.any(OperationLogTbl.class), Mockito.anyBoolean());
        when(conflictLogService.getConflictTrxLogView(any(ConflictTrxLogQueryParam.class))).thenReturn(Lists.newArrayList());
        MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v2/log/conflict/trx??dbName=testdb&tableName=table&beginHandleTime=1702224000000&endHandleTime=1702310400000&likeSearch=false&brief=0&pageReq.pageSize=10&pageReq.pageIndex=3")
                        .accept(MediaType.APPLICATION_JSON)).andReturn();
        assertNormalResponse(mvcResult);
        verify(logRecordService, times(1)).record(Mockito.any(OperationLogTbl.class), Mockito.anyBoolean());
        // fail
        when(conflictLogService.getConflictTrxLogView(any(ConflictTrxLogQueryParam.class))).thenThrow(new SQLException("sql exception"));
        mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v2/log/conflict/trx??dbName=testdb&tableName=table&beginHandleTime=1702224000000&endHandleTime=1702310400000&likeSearch=false&brief=0&pageReq.pageSize=10&pageReq.pageIndex=3")
                .accept(MediaType.APPLICATION_JSON)).andReturn();
        assertFailResponse(mvcResult);
        verify(logRecordService, times(2)).record(Mockito.any(OperationLogTbl.class), Mockito.anyBoolean());
    }

    protected void assertNormalResponse(MvcResult mvcResult) throws Exception {
        MockHttpServletResponse response = mvcResult.getResponse();
        response.setContentType("application/json");
        int status = response.getStatus();
        String responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        ApiResult result = objectMapper.readValue(responseStr, ApiResult.class);
        Assert.assertEquals(0, result.getStatus().intValue());
    }

    protected void assertFailResponse(MvcResult mvcResult) throws Exception {
        MockHttpServletResponse response = mvcResult.getResponse();
        response.setContentType("application/json");
        int status = response.getStatus();
        String responseStr = response.getContentAsString();
        Assert.assertEquals(200, status);
        ApiResult result = objectMapper.readValue(responseStr, ApiResult.class);
        Assert.assertEquals(1, result.getStatus().intValue());
    }
    
    

    
}