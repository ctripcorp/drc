package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

import java.util.List;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

public class MonitorServiceV2ImplTest {
    @Mock
    Logger logger;
    @Mock
    MhaTblV2Dao mhaTblV2Dao;
    @Mock
    DefaultConsoleConfig consoleConfig;
    @Mock
    MetaProviderV2 metaProviderV2;
    @InjectMocks
    MonitorServiceV2Impl monitorServiceV2Impl;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }


    @Test
    public void testGetDestMhaNamesToBeMonitored() throws Exception {
        MhaTblV2 mhaTblV2 = new MhaTblV2();
        mhaTblV2.setMhaName("srcMha");
        when(mhaTblV2Dao.queryBy(any())).thenReturn(List.of(mhaTblV2));
        when(consoleConfig.getRegion()).thenReturn("region");
        Drc drc = new Drc();
        Dc dc = new Dc();
        dc.addDbCluster(new DbCluster().setId("id1").setMhaName("dstMha").addApplier(new Applier().setTargetMhaName("srcMha")));
        dc.addDbCluster(new DbCluster().setId("id2").setMhaName("dstMha2").addApplier(new Applier().setTargetMhaName("srcMhaNotMonitored1")));
        dc.addDbCluster(new DbCluster().setId("id3").setMhaName("dstMha3"));
        drc.addDc(dc);

        when(metaProviderV2.getDrc()).thenReturn(drc);

        List<String> result = monitorServiceV2Impl.getDestMhaNamesToBeMonitored();
        Assert.assertEquals(List.of("dstMha"), result);
    }

}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme