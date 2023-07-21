package com.ctrip.framework.drc.console.service.monitor.impl;

import com.ctrip.framework.drc.console.AbstractTest;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.MhaTblDao;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.service.impl.openapi.OpenService;
import com.ctrip.framework.drc.console.service.v2.DrcDoubleWriteService;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.console.vo.response.MhaNamesResponseVo;
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
import java.util.List;

/**
 * Created by jixinwang on 2021/8/5
 */
public class MonitorServiceImplTest extends AbstractTest {

    @InjectMocks
    private MonitorServiceImpl monitorService;

    @Mock
    private DbClusterSourceProvider sourceProvider;

    @Mock
    private DefaultConsoleConfig consoleConfig;

    @Mock
    private OpenService openService;

    @Mock
    private MhaTblDao mhaTblDao;

    @Mock
    private DalUtils dalUtils;

    @Mock
    private DrcDoubleWriteService drcDoubleWriteService;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void queryMhaNamesToBeMonitored() throws SQLException {
        monitorService.queryMhaNamesToBeMonitored();
    }

    @Test
    public void getMhaNamesToBeMonitoredFromRemote() throws SQLException {
        Mockito.when(sourceProvider.getLocalDcName()).thenReturn("dc1");
        Mockito.when(consoleConfig.getLocalConfigCloudDc()).thenReturn(Sets.newHashSet("dc2"));
        Mockito.when(consoleConfig.getPublicCloudRegion()).thenReturn(Sets.newHashSet("region1"));
        Mockito.when(consoleConfig.getRegion()).thenReturn("region1");

        Mockito.when(consoleConfig.getCenterRegionUrl()).thenReturn("centerRegionConsoleUrl");
        MhaNamesResponseVo mhaNamesResponseVo = new MhaNamesResponseVo();
        mhaNamesResponseVo.setStatus(0);
        List<String> openServiceResult = Lists.newArrayList("mhaName1");
        mhaNamesResponseVo.setData(openServiceResult);
        Mockito.doReturn(mhaNamesResponseVo).when(openService).getMhaNamesToBeMonitored(Mockito.anyString());
        List<String> mhaNamesToBeMonitored = monitorService.getMhaNamesToBeMonitored();
        Assert.assertEquals(1, mhaNamesToBeMonitored.size());


        Mockito.when(consoleConfig.getLocalConfigCloudDc()).thenReturn(Sets.newHashSet("dc1"));
        Mockito.when(consoleConfig.getLocalDcMhaNamesToBeMonitored()).thenReturn(Lists.newArrayList("mah1", "mha2"));
        Assert.assertEquals(2, monitorService.getMhaNamesToBeMonitored().size());
    }

    @Test
    public void testTestSwitchMonitors() throws Exception {
        Mockito.when(mhaTblDao.queryByMhaName(Mockito.anyString(), Mockito.anyInt())).thenReturn(new MhaTbl());
        Mockito.when(mhaTblDao.update(Mockito.any(MhaTbl.class))).thenReturn(1);
        Mockito.doNothing().when(drcDoubleWriteService).switchMonitor(Mockito.anyLong());

        monitorService.switchMonitors("mha1", "off");
    }

    @Test
    public void testTestQueryMhaNamesToBeMonitored() throws SQLException {
        MhaTbl mhaTbl = new MhaTbl();
        mhaTbl.setMhaName("mha1");
        Mockito.when(mhaTblDao.queryBy(Mockito.any(MhaTbl.class))).thenReturn(Lists.newArrayList(mhaTbl));
        Mockito.when(consoleConfig.getDrcDoubleWriteSwitch()).thenReturn("false");

        List<String> names = monitorService.queryMhaNamesToBeMonitored();
        Assert.assertEquals(1, names.size());

    }

}