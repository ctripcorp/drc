package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.monitor.DefaultCurrentMetaManager;
import com.ctrip.framework.drc.console.service.v2.DbMetaCorrectService;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.config.console.dto.DbEndpointDto;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static com.ctrip.framework.drc.console.monitor.MockTest.times;
import static com.ctrip.framework.drc.console.utils.UTConstants.*;
import static org.mockito.Mockito.verify;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-24
 */
public class SwitchServiceImplTest {

    @InjectMocks private SwitchServiceImpl switchService;

    @Mock private DefaultCurrentMetaManager currentMetaManager;

    @Mock private DbMetaCorrectService dbMetaCorrectService;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        Mockito.doReturn(ApiResult.getSuccessInstance("")).when(dbMetaCorrectService).mhaMasterDbChange(Mockito.anyString(), Mockito.anyString(), Mockito.anyInt());
        Mockito.doNothing().when(currentMetaManager).updateMasterMySQL(Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testSwitchUpdateDb() {
        DbEndpointDto dto = new DbEndpointDto(IP1, MYSQL_PORT);
        ApiResult result = switchService.switchUpdateDb(CLUSTER_ID1, dto);
        verify(currentMetaManager, times(1)).updateMasterMySQL(Mockito.anyString(), Mockito.any());
        Assert.assertEquals(ResultCode.HANDLE_SUCCESS.getCode(), result.getStatus().intValue());
    }
}
