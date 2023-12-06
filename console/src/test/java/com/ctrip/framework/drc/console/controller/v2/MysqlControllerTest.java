package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.param.mysql.DdlHistoryEntity;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;

/**
 * Created by dengquanliang
 * 2023/12/6 17:21
 */
public class MysqlControllerTest {

    @InjectMocks
    private MysqlController mysqlController;
    @Mock
    private MysqlServiceV2 mysqlServiceV2;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testInsertDdlHistory() throws SQLException {
        Mockito.when(mysqlServiceV2.insertDdlHistory(Mockito.any())).thenReturn(1);
        ApiResult<Integer> result = mysqlController.insertDdlHistory(new DdlHistoryEntity());
        Assert.assertTrue(result.getData() == 1);

        Mockito.when(mysqlServiceV2.insertDdlHistory(Mockito.any())).thenThrow(ConsoleExceptionUtils.message("error"));
        result = mysqlController.insertDdlHistory(new DdlHistoryEntity());
        Assert.assertNull(result.getData());
    }
}
