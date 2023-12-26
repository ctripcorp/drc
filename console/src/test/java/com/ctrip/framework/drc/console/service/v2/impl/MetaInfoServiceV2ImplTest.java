package com.ctrip.framework.drc.console.service.v2.impl;

import com.alibaba.fastjson.JSON;
import com.ctrip.framework.drc.console.dao.BuTblDao;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.entity.BuTbl;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.RegionTbl;
import com.ctrip.framework.drc.console.dao.v2.RegionTblDao;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MetaInfoServiceV2ImplTest {

    @InjectMocks
    private MetaInfoServiceV2Impl metaInfoServiceV2;

    @Mock
    private DcTblDao dcTblDao;

    @Mock
    private BuTblDao buTblDao;

    @Mock
    private DbTblDao dbTblDao;
    @Mock
    private RegionTblDao regionTblDao;


    @Before
    public void setup() throws SQLException {
        List<DcDo> dcDos = getDcDos();
        when(dcTblDao.queryAll()).thenReturn(getAllDcs());
        when(buTblDao.queryAll()).thenReturn(getAllBus());
        when(regionTblDao.queryAll()).thenReturn(getRegions());
    }

    @Test
    public void queryAllDcWithCache() {
        Map<Long, DcTbl> dcTblMap = getAllDcs().stream().collect(Collectors.toMap(DcTbl::getId, e -> e));
        Map<Long, RegionTbl> regionTblMap = getRegions().stream().collect(Collectors.toMap(RegionTbl::getId, e -> e));

        List<DcDo> dcDos = metaInfoServiceV2.queryAllDcWithCache();
        Assert.assertEquals(dcTblMap.size(), dcDos.size());
        // compare
        for (DcDo dcDo : dcDos) {
            DcTbl dcTbl = dcTblMap.get(dcDo.getDcId());
            RegionTbl regionTbl = regionTblMap.get(dcDo.getRegionId());

            Assert.assertEquals(dcTbl.getDcName(), dcDo.getDcName());
            Assert.assertEquals(regionTbl.getRegionName(), dcDo.getRegionName());
        }

    }

    @Test
    public void queryAllRegionWithCache() {
        List<RegionTbl> regionTbls = metaInfoServiceV2.queryAllRegionWithCache();

        Assert.assertTrue(regionTbls.size() != 0);
    }

    @Test
    public void queryAllBuWithCache() {
        List<BuTbl> buTbls = metaInfoServiceV2.queryAllBuWithCache();

        Assert.assertTrue(buTbls.size() != 0);
    }

    @Test
    public void queryAllBu() {
        List<BuTbl> buTbls = metaInfoServiceV2.queryAllBu();
        Assert.assertTrue(buTbls.size() != 0);
    }

    @Test
    public void queryAllRegion() {
        List<RegionTbl> regionTbls = metaInfoServiceV2.queryAllRegion();
        Assert.assertTrue(regionTbls.size() != 0);

    }

    @Test
    public void queryAllDc() {
        List<DcDo> dcDos = metaInfoServiceV2.queryAllDc();
        Assert.assertTrue(dcDos.size() != 0);

    }

    @Test
    public void queryAllDbBuCode() throws SQLException {
        when(dbTblDao.queryAllExist()).thenReturn(getDbs());
        List<String> buCodes = metaInfoServiceV2.queryAllDbBuCode();
        Assert.assertTrue(buCodes.size() != 0);

    }

    private List<DbTbl> getDbs() {
        String json = "[\n" +
                "  {\n" +
                "    \"id\": 1,\n" +
                "    \"dbName\": \"db1\",\n" +
                "    \"dbOwner\": \"test_48e092105ced\",\n" +
                "    \"buCode\": \"test_7fa0f7891f9e\",\n" +
                "    \"buName\": \"BBZTEST\",\n" +
                "    \"trafficSendLastTime\": 85,\n" +
                "    \"isDrc\": 1\n" +
                "  }\n" +
                "]";
        return JSON.parseArray(json, DbTbl.class);
    }

    private static List<BuTbl> getAllBus() {
        String json = "[{\"id\":3,\"buName\":\"BBZ\"},{\"id\":4,\"buName\":\"IBU\"}]";
        return JSON.parseArray(json, BuTbl.class);
    }

    private static List<DcTbl> getAllDcs() {
        String json = "[{\"id\":1,\"dcName\":\"shaoy\",\"regionName\":\"sha\"},{\"id\":2,\"dcName\":\"sinaws\",\"regionName\":\"sin\"}]";
        return JSON.parseArray(json, DcTbl.class);
    }

    private static List<DcDo> getDcDos() {
        String json = "[{\"dcId\":1,\"dcName\":\"shaoy\",\"regionId\":1,\"regionName\":\"sha\"},{\"dcId\":2,\"dcName\":\"sinaws\",\"regionId\":2,\"regionName\":\"sin\"}]";
        return JSON.parseArray(json, DcDo.class);
    }

    private static List<RegionTbl> getRegions() {
        String json = "[{\"id\":1,\"regionName\":\"sha\"},{\"id\":2,\"regionName\":\"sin\"}]";
        return JSON.parseArray(json, RegionTbl.class);
    }



}