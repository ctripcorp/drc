package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.dto.ConflictTransactionLog;
import com.ctrip.framework.drc.console.dto.LogDto;
import com.ctrip.framework.drc.console.dto.LogHandleDto;
import com.ctrip.framework.drc.console.service.LogService;
import com.ctrip.framework.drc.console.service.checker.ConflictLogChecker;
import com.ctrip.framework.drc.console.service.impl.UnitServiceImpl;
import com.ctrip.framework.drc.console.utils.JsonUtils;
import com.ctrip.platform.dal.dao.DalHints;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;

/**
 * Created by jixinwang on 2020/6/22
 */
public class LogControllerTest {

    protected ObjectMapper objectMapper = new ObjectMapper();

    private MockMvc mvc;

    @InjectMocks
    private LogController controller;

    @Mock
    private LogService logService;

    @Mock
    private ConflictLogChecker conflictLogChecker;

    @Mock
    private UnitServiceImpl unitService = new UnitServiceImpl();

    @Before
    public void setUp() {
        /** initialization */
        MockitoAnnotations.openMocks(this);
        /** build mvc env */
        mvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    public void testUploadConflictLog() throws Exception {
        List<ConflictTransactionLog> conflictTransactionLogList = new ArrayList<ConflictTransactionLog>();
        ConflictTransactionLog conflictTransactionLog = new ConflictTransactionLog();
        conflictTransactionLog.setClusterName("testClusterName");
        conflictTransactionLogList.add(conflictTransactionLog);

        Mockito.when(conflictLogChecker.inBlackList(anyString())).thenReturn(false);

        MvcResult mvcResult = mvc.perform(post("/api/drc/v1/logs/conflicts")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtils.toJson(conflictTransactionLogList))
                .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        Assert.assertEquals(200, status);
    }

    @Test
    public void testUploadSampleLog() throws Exception {
        LogDto logDto = new LogDto();
        logDto.setSrcDcName("src");
        logDto.setDestDcName("dest");
        logDto.setUserId("user");

        doNothing().when(logService).uploadSampleLog(Mockito.any(LogDto.class));
        MvcResult mvcResult = mvc.perform(post("/api/drc/v1/logs/samples")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtils.toJson(logDto))
                .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        Assert.assertEquals(200, status);
    }

    @Test
    public void testGetLogs() throws Exception {
        Map<String, Object> map = new HashMap<>();
        when(logService.getLogs(1, 10)).thenReturn(map);
        MvcResult mvcResult = mvc.perform(get("/api/drc/v1/logs/1/10")
                .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        Assert.assertEquals(200, status);
    }

    @Test
    public void testGetLogsSearch() throws Exception {
        Map<String, Object> map = new HashMap<>();
        when(logService.getLogs(1, 10)).thenReturn(map);
        MvcResult mvcResult = mvc.perform(get("/api/drc/v1/logs/1/10?keyWord=drc")
                .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        Assert.assertEquals(200, status);
    }

    @Test
    public void testDeleteLog() throws Exception {
        doNothing().when(logService).deleteLog(Mockito.any(DalHints.class));
        MvcResult mvcResult = mvc.perform(delete("/api/drc/v1/logs")
                .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        Assert.assertEquals(200, status);
    }

    @Test
    public void testSelectRecord() throws Exception {
//        LogHandleDto logHandleDto = new LogHandleDto();
//        logHandleDto.setCurrentDbClusterId("clusterId");
//        logHandleDto.setCurrentDcName("dcName");
//        logHandleDto.setCurrentSql("sql");
//
//        doReturn("ret").when(logService).selectRecord(Mockito.any(LogHandleDto.class));
//        MvcResult mvcResult = mvc.perform(post("/api/drc/v1/logs/record/select")
//                .contentType(MediaType.APPLICATION_JSON).content(JsonUtils.toJson(logHandleDto))
//                .accept(MediaType.APPLICATION_JSON))
//                .andReturn();
//        int status = mvcResult.getResponse().getStatus();
//        Assert.assertEquals(200, status);
    }

    @Test
    public void testUpdateRecord() throws Exception {
        LogHandleDto logHandleDto = new LogHandleDto();
        logHandleDto.setCurrentDbClusterId("clusterId");
        logHandleDto.setCurrentDcName("dcName");
        logHandleDto.setCurrentSql("sql");

        doNothing().when(logService).updateRecord(Mockito.any(Map.class));
        MvcResult mvcResult = mvc.perform(post("/api/drc/v1/logs/record/conflicts/update")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtils.toJson(logHandleDto))
                .accept(MediaType.APPLICATION_JSON))
                .andReturn();
        int status = mvcResult.getResponse().getStatus();
        Assert.assertEquals(200, status);
    }
}
