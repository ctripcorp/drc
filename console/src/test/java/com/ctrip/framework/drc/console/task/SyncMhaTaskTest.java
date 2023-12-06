package com.ctrip.framework.drc.console.task;

import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.impl.DalServiceImpl;
import com.ctrip.framework.foundation.Env;
import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Map;

public class SyncMhaTaskTest {
    
    @InjectMocks
    SyncMhaTask syncMhaTask;

    @Mock
    private DalServiceImpl dalServicedalService;

    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;
    
    @Before
    public void setUp(){
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testScheduledTask() throws Exception {
        // not leader
        syncMhaTask.scheduledTask();
        
        
        
        //leader and fail
        Mockito.when(monitorTableSourceProvider.getSyncMhaSwitch()).thenReturn("on");
        Mockito.when(dalServicedalService.getMhaList(Mockito.any(Env.class))).thenThrow(new RuntimeException("error"));
        syncMhaTask.isleader();
        syncMhaTask.scheduledTask();;
        
        // leader and success
        Map<String, MhaInstanceGroupDto> mhaInstanceGroupMap = Maps.newHashMap();
        Mockito.when(dalServicedalService.getMhaList(Mockito.any(Env.class))).thenReturn(mhaInstanceGroupMap);
        syncMhaTask.scheduledTask();
        
        
        
    }
}