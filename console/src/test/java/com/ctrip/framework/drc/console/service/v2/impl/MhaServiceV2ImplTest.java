package com.ctrip.framework.drc.console.service.v2.impl;

import com.alibaba.fastjson.JSON;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.param.v2.MhaQuery;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.google.common.collect.Lists;
import org.apache.commons.collections4.MapUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class MhaServiceV2ImplTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());


    @InjectMocks
    private MhaServiceV2Impl mhaServiceV2;

    @Mock
    private MhaTblV2Dao mhaTblV2Dao;

    @Mock
    private MetaInfoServiceV2 metaInfoServiceV2;

    @Before
    public void setup() throws SQLException {
        List<DcDo> dcDos = getDcDos();
        Mockito.when(metaInfoServiceV2.queryAllDcWithCache()).thenReturn(dcDos);
    }


    @Test
    public void queryEmptyInput() {
        Map<Long, MhaTblV2> emptyResult = mhaServiceV2.query(null, null, null);
        assertTrue(MapUtils.isEmpty(emptyResult));
    }

    @Test
    public void queryWithBuId() throws SQLException {
        long buId = 1L;

        MhaQuery mhaQuery = new MhaQuery();
        mhaQuery.setBuId(buId);
        List<MhaTblV2> allMhaData = getAllMhaData();
        List<MhaTblV2> expectResultList = allMhaData.stream().filter(e -> Objects.equals(e.getBuId(), buId)).collect(Collectors.toList());

        Mockito.when(mhaTblV2Dao.query(mhaQuery)).thenReturn(expectResultList);
        Map<Long, MhaTblV2> result = mhaServiceV2.query(null, buId, null);
        assertResult(expectResultList, result);
    }

    @Test
    public void queryWithRegionId() throws SQLException {
        long regionId = 1L;

        List<MhaTblV2> allMhaData = getAllMhaData();
        List<Long> dcIdList = getDcDos().stream().filter(e -> Objects.equals(e.getRegionId(), regionId)).map(DcDo::getDcId).collect(Collectors.toList());
        List<MhaTblV2> expectResultList = allMhaData.stream().filter(e -> dcIdList.contains(e.getDcId())).collect(Collectors.toList());

        MhaQuery mhaQuery = new MhaQuery();
        mhaQuery.setDcIdList(dcIdList);
        Mockito.when(mhaTblV2Dao.query(mhaQuery)).thenReturn(expectResultList);

        Map<Long, MhaTblV2> result = mhaServiceV2.query(null, null, regionId);

        assertResult(expectResultList, result);
    }

    @Test
    public void queryWithMhaName() throws SQLException {
        String mhaName = "mha1";

        List<MhaTblV2> allMhaData = getAllMhaData();
        List<MhaTblV2> expectResultList = allMhaData.stream().filter(e -> e.getMhaName().contains(mhaName)).collect(Collectors.toList());

        MhaQuery mhaQuery = new MhaQuery();
        mhaQuery.setContainMhaName(mhaName);
        Mockito.when(mhaTblV2Dao.query(mhaQuery)).thenReturn(expectResultList);

        Map<Long, MhaTblV2> result = mhaServiceV2.query(mhaName, null, null);

        assertResult(expectResultList, result);
    }

    @Test(expected = ConsoleException.class)
    public void queryWithSQLException() throws SQLException {
        Mockito.when(mhaTblV2Dao.query(Mockito.any())).thenThrow(SQLException.class);
        Map<Long, MhaTblV2> result = mhaServiceV2.query("test", null, null);
    }

    @Test
    public void queryMhaByIds() throws SQLException {
        List<MhaTblV2> allMhaData = getAllMhaData();
        List<List<Long>> mhaIdLitsList = Lists.newArrayList(
                Lists.newArrayList(),
                Lists.newArrayList(1L),
                Lists.newArrayList(1L, 2L, 3L)
        );

        for (List<Long> mhaIds : mhaIdLitsList) {
            List<MhaTblV2> expectResultList = allMhaData.stream().filter(e -> mhaIds.contains(e.getId())).collect(Collectors.toList());
            Mockito.when(mhaTblV2Dao.queryByIds(mhaIds)).thenReturn(expectResultList);
            Map<Long, MhaTblV2> result = mhaServiceV2.queryMhaByIds(mhaIds);
            logger.info("input:{}, result:{}", mhaIds, result);
            assertResult(expectResultList, result);
        }
    }

    @Test(expected = ConsoleException.class)
    public void queryMhaByIdsException() throws SQLException {
        Mockito.when(mhaTblV2Dao.queryByIds(Mockito.any())).thenThrow(SQLException.class);
        Map<Long, MhaTblV2> result = mhaServiceV2.queryMhaByIds(Lists.newArrayList(1L));
    }

    private static void assertResult(List<MhaTblV2> expectResult, Map<Long, MhaTblV2> result) {
        Assert.assertEquals(result.size(), expectResult.size());
        for (MhaTblV2 mhaTblV2 : expectResult) {
            Assert.assertTrue(result.containsKey(mhaTblV2.getId()));
        }
    }

    private static List<MhaTblV2> getAllMhaData() {
        return JSON.parseArray("[{\"id\":1,\"mhaName\":\"mha1\",\"dcId\":1,\"buId\":1},{\"id\":2,\"mhaName\":\"mha2\",\"dcId\":2,\"buId\":1},{\"id\":3,\"mhaName\":\"mha3\",\"dcId\":1,\"buId\":2}]", MhaTblV2.class);
    }

    private static List<DcDo> getDcDos() {
        return JSON.parseArray("[{\"dcId\":1,\"dcName\":\"test\",\"regionId\":1,\"regionName\":\"test\"}]", DcDo.class);
    }
}