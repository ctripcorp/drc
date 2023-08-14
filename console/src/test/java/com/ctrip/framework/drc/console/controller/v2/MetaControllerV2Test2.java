package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.dao.entity.BuTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.RegionTbl;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.MetaGrayService;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.impl.migrate.DbClusterCompareRes;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

import java.util.List;

import static org.mockito.Mockito.*;

public class MetaControllerV2Test2 {
    @Mock
    Logger logger;
    @Mock
    MetaProviderV2 metaProviderV2;
    @Mock
    MetaGrayService metaServiceV2;
    @Mock
    MetaInfoServiceV2 metaInfoServiceV2;
    @InjectMocks
    MetaControllerV2 metaControllerV2;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testGetAllMetaData() throws Exception {
        when(metaProviderV2.getDrc()).thenReturn(new Drc());

        String result = metaControllerV2.getAllMetaData("refresh");
        Assert.assertEquals("replaceMeWithExpectedResult", result);
    }

    @Test
    public void testCompareOldNewMeta() throws Exception {
        when(metaServiceV2.compareDrcMeta()).thenReturn("compareDrcMetaResponse");

        ApiResult<String> result = metaControllerV2.compareOldNewMeta();
        Assert.assertEquals(new ApiResult<String>(), result);
    }

    @Test
    public void testCompareOldNewMeta2() throws Exception {
        when(metaServiceV2.compareDbCluster(anyString())).thenReturn(new DbClusterCompareRes());

        ApiResult<DbClusterCompareRes> result = metaControllerV2.compareOldNewMeta("dbclusterId");
        Assert.assertEquals(new ApiResult<DbClusterCompareRes>(), result);
    }



    @Test
    public void testGetAllBuTbls() throws Exception {
        when(metaInfoServiceV2.queryAllBuWithCache()).thenReturn(List.of(new BuTbl()));

        ApiResult<List<BuTbl>> result = metaControllerV2.getAllBuTbls();
        Assert.assertEquals(new ApiResult<List<BuTbl>>(), result);
    }

    @Test
    public void testGetAllRegionTbls() throws Exception {
        when(metaInfoServiceV2.queryAllRegionWithCache()).thenReturn(List.of(new RegionTbl()));

        ApiResult<List<RegionTbl>> result = metaControllerV2.getAllRegionTbls();
        Assert.assertEquals(new ApiResult<List<RegionTbl>>(), result);
    }

    @Test
    public void testGetAllDcs() throws Exception {
        when(metaInfoServiceV2.queryAllDcWithCache()).thenReturn(List.of(new DcDo()));

        ApiResult<List<DcDo>> result = metaControllerV2.getAllDcs();
        Assert.assertEquals(new ApiResult<List<DcDo>>(), result);
    }
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme