package com.ctrip.framework.drc.console.controller.log;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.ctrip.framework.drc.console.param.log.OperationLogQueryParam;
import com.ctrip.framework.drc.console.service.log.OperationLogService;
import com.ctrip.framework.drc.console.vo.log.OperationLogView;
import com.ctrip.framework.drc.console.vo.log.OptionView;
import com.ctrip.framework.drc.core.http.ApiResult;
import java.sql.SQLException;
import java.util.List;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class OperationLogControllerTest {
    
    @InjectMocks
    private OperationLogController operationLogController;
    
    @Mock
    private OperationLogService operationLogService;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testGetOperationLogView() throws SQLException {
        when(operationLogService.getOperationLogView(Mockito.any(OperationLogQueryParam.class))).thenReturn(Lists.newArrayList());
        OperationLogQueryParam operationLogQueryParam = new OperationLogQueryParam();
        operationLogQueryParam.setOperationKeyword("test");
        operationLogQueryParam.getOperationKeyword();
        ApiResult<List<OperationLogView>> operationLogView = operationLogController.getOperationLogView(operationLogQueryParam);
        assertEquals(0,operationLogView.getStatus().intValue());
        
        doThrow(new SQLException()).when(operationLogService).getOperationLogView(Mockito.any(OperationLogQueryParam.class));
        ApiResult<List<OperationLogView>> operationLogView2 = operationLogController.getOperationLogView(
                new OperationLogQueryParam());
        assertEquals(1,operationLogView2.getStatus().intValue());
    }

    @Test
    public void testGetOperationLogCount() throws SQLException {
        when(operationLogService.gerOperationLogCount(Mockito.any(OperationLogQueryParam.class))).thenReturn(10L);
        ApiResult<Long> operationLogCount = operationLogController.getOperationLogCount(new OperationLogQueryParam());
        assertEquals(10,operationLogCount.getData().intValue());

        doThrow(new SQLException()).when(operationLogService).gerOperationLogCount(Mockito.any(OperationLogQueryParam.class));
        ApiResult<Long> operationLogCount2 = operationLogController.getOperationLogCount(new OperationLogQueryParam());
        assertEquals(1,operationLogCount2.getStatus().intValue());
    }

    @Test
    public void testGetAllType() {
        ApiResult<List<OptionView>> allType = operationLogController.getAllType();
        assertEquals(0,allType.getStatus().intValue());
        assertEquals(6,allType.getData().size());
    }

    @Test
    public void testGetAllAttribute() {
        ApiResult<List<OptionView>> allAttribute = operationLogController.getAllAttribute();
        assertEquals(0,allAttribute.getStatus().intValue());
        assertEquals(4,allAttribute.getData().size());
    }
}