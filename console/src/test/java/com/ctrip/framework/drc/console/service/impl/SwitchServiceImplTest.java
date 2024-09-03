package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.monitor.DefaultCurrentMetaManager;
import com.ctrip.framework.drc.console.monitor.delay.task.ListenReplicatorTask;
import com.ctrip.framework.drc.console.service.broadcast.HttpNotificationBroadCast;
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
import org.springframework.web.bind.annotation.RequestMethod;

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
    
    @Mock private ListenReplicatorTask listenReplicatorTask;
    
    @Mock private HttpNotificationBroadCast broadCast;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        Mockito.doNothing().when(currentMetaManager).updateMasterMySQL(Mockito.anyString(), Mockito.any());
        Mockito.doNothing().when(broadCast).broadcast(Mockito.anyString(), Mockito.any(RequestMethod.class), Mockito.anyString());
    }

    @Test
    public void testSwitchUpdateDb() {
        Mockito.doReturn(ApiResult.getSuccessInstance("")).when(dbMetaCorrectService).mhaMasterDbChange(Mockito.anyString(), Mockito.anyString(), Mockito.anyInt());
        switchService.switchUpdateDb(CLUSTER_ID1, IP1 + ":" + MYSQL_PORT, true);
        verify(currentMetaManager, times(1)).updateMasterMySQL(Mockito.anyString(), Mockito.any());
        verify(broadCast, times(1)).broadcast(Mockito.anyString(), Mockito.any(RequestMethod.class), Mockito.anyString());
    }
    
    @Test
    public void testSwitchListenReplicator() {
        Mockito.doNothing().when(listenReplicatorTask).switchListenReplicator(Mockito.anyString(), Mockito.anyString(), Mockito.anyInt());
        switchService.switchListenReplicator(CLUSTER_ID1, IP1 + ":" + 8083, true);
        verify(listenReplicatorTask, times(1)).switchListenReplicator(Mockito.anyString(), Mockito.anyString(), Mockito.anyInt());
        verify(broadCast, times(1)).broadcast(Mockito.anyString(), Mockito.any(RequestMethod.class), Mockito.anyString());
    }
}
