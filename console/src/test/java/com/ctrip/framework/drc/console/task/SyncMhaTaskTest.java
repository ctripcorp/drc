package com.ctrip.framework.drc.console.task;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.impl.DalServiceImpl;
import com.ctrip.framework.drc.console.service.v2.PojoBuilder;
import com.ctrip.framework.drc.core.monitor.reporter.CatEventMonitor;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.foundation.Env;
import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.util.Map;

public class SyncMhaTaskTest {
    
    @InjectMocks
    SyncMhaTask syncMhaTask;

    @Mock
    private DalServiceImpl dalServicedalService;

    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Mock
    private MhaTblV2Dao mhaTblV2Dao;

    @Mock
    CatEventMonitor catEventMonitor;
    
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

    @Test
    public void testOfflineMha() throws Exception{
        MockedStatic<DefaultEventMonitorHolder> mockedStatic;
        mockedStatic = Mockito.mockStatic(DefaultEventMonitorHolder.class);
        mockedStatic.when(DefaultEventMonitorHolder::getInstance).thenReturn(catEventMonitor);
        Mockito.when(mhaTblV2Dao.queryBy(Mockito.any(MhaTblV2.class))).thenReturn(PojoBuilder.getMhaTblV2s());
        syncMhaTask.updateAllMhaInstanceGroup(Maps.newHashMap());
        mockedStatic.verify(Mockito.times(2), DefaultEventMonitorHolder::getInstance);
        mockedStatic.close();
    }
}