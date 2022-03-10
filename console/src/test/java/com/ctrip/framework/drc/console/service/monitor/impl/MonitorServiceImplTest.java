package com.ctrip.framework.drc.console.service.monitor.impl;

import com.ctrip.framework.drc.console.AbstractTest;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.service.impl.openapi.OpenService;
import com.ctrip.framework.drc.console.vo.response.MhaNamesResponseVo;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by jixinwang on 2021/8/5
 */
public class MonitorServiceImplTest extends AbstractTest {

    @Mock
    private DbClusterSourceProvider sourceProvider;

    @Mock
    private DefaultConsoleConfig consoleConfig;

    @Mock
    private OpenService openService;

    @InjectMocks
    private MonitorServiceImpl monitorService;


    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void switchMonitors() {

    }

    @Test
    public void queryMhaNamesToBeMonitored() throws SQLException {
        monitorService.queryMhaNamesToBeMonitored();
    }

    @Test
    public void getMhaNamesToBeMonitoredFromRemote() throws SQLException {
        Mockito.when(sourceProvider.getLocalDcName()).thenReturn("publicDc");
        Mockito.when(consoleConfig.getPublicCloudDc()).thenReturn(Sets.newHashSet("publicDc"));

        Map<String, String> wormholes = Maps.newHashMap();
        wormholes.put("remoteDcName", "remoteWormHoleUri");
        Mockito.when(consoleConfig.getConsoleDcInfos()).thenReturn(wormholes);

        MhaNamesResponseVo mhaNamesResponseVo = new MhaNamesResponseVo();
        mhaNamesResponseVo.setStatus(0);
        List<String> openServiceResult = Lists.newArrayList("mhaName1");
        mhaNamesResponseVo.setData(openServiceResult);
        Mockito.doReturn(mhaNamesResponseVo).when(openService).getMhaNamesToBeMonitored(Mockito.anyString());
        List<String> mhaNamesToBeMonitored = monitorService.getMhaNamesToBeMonitored();
        Assert.assertEquals(1, mhaNamesToBeMonitored.size());
        
        Mockito.when(consoleConfig.getLocalConfigCloudDc()).thenReturn(Sets.newHashSet("publicDc"));
        Mockito.when(consoleConfig.getLocalDcMhaNamesToBeMonitored()).thenReturn(Lists.newArrayList("mah1","mha2"));
        Assert.assertEquals(2,monitorService.getMhaNamesToBeMonitored().size());
    }
}