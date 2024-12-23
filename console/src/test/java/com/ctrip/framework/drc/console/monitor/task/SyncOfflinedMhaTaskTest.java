package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.service.v2.PojoBuilder;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.core.monitor.reporter.CatEventMonitor;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

/**
 * Created by shiruixin
 * 2024/12/23 16:32
 */
public class SyncOfflinedMhaTaskTest {
    @InjectMocks
    private SyncOfflinedMhaTask task;
    @Mock
    private DefaultConsoleConfig consoleConfig;
    @Mock
    private ResourceService resourceService;
    @Mock
    private MhaTblV2Dao mhaTblV2Dao;
    @Mock
    CatEventMonitor catEventMonitor;

    MockedStatic<DefaultEventMonitorHolder> mockedStatic;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        mockedStatic = Mockito.mockStatic(DefaultEventMonitorHolder.class);
        mockedStatic.when(DefaultEventMonitorHolder::getInstance).thenReturn(catEventMonitor);
    }

    @After
    public void tearDown() {
        mockedStatic.close();
    }

    @Test
    public void testScheduledTask () throws Exception {
        Mockito.when(resourceService.getMhaInstanceGroupsInAllRegions()).thenReturn(PojoBuilder.getMhaInstanceGroups());
        Mockito.when(mhaTblV2Dao.queryAllExist()).thenReturn(PojoBuilder.getMhaTblV2s());
        task.check();
        mockedStatic.verify(Mockito.times(1), DefaultEventMonitorHolder::getInstance);
    }
}