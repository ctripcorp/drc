package com.ctrip.framework.drc.console.service.v2.impl;

import static com.ctrip.framework.drc.console.service.v2.MetaGeneratorBuilder.getReplicatorTbls;
import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.getDcTbls;
import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.getMhaTblV2s;
import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.getReplicatorGroupTbls;
import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.getResourceTbls;
import static org.junit.Assert.assertTrue;

import com.alibaba.fastjson.JSON;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.MachineTblDao;
import com.ctrip.framework.drc.console.dao.MessengerGroupTblDao;
import com.ctrip.framework.drc.console.dao.MessengerTblDao;
import com.ctrip.framework.drc.console.dao.ReplicatorGroupTblDao;
import com.ctrip.framework.drc.console.dao.ReplicatorTblDao;
import com.ctrip.framework.drc.console.dao.ResourceTblDao;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerTbl;
import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.param.v2.MhaQuery;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.utils.EnvUtils;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.service.ops.OPSApiService;
import com.ctrip.framework.drc.core.service.statistics.traffic.HickWallMhaReplicationDelayEntity;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.FileUtils;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
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
import org.xml.sax.SAXException;

@RunWith(MockitoJUnitRunner.class)
public class MhaServiceV2ImplTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());


    @InjectMocks
    private MhaServiceV2Impl mhaServiceV2;

    @Mock
    private MhaTblV2Dao mhaTblV2Dao;

    @Mock
    private MetaInfoServiceV2 metaInfoServiceV2;

    @Mock
    private ReplicatorGroupTblDao replicatorGroupTblDao;

    @Mock
    private ResourceTblDao resourceTblDao;

    @Mock
    private ReplicatorTblDao replicatorTblDao;

    @Mock
    private DcTblDao dcTblDao;

    @Mock
    private MachineTblDao machineTblDao;

    @Mock
    private MessengerTblDao messengerTblDao;

    @Mock
    private MessengerGroupTblDao messengerGroupTblDao;

    @Mock
    private OPSApiService opsApiServiceImpl;
    
    @Mock
    private DomainConfig domainConfig;

    @Mock
    private MetaProviderV2 metaProviderV2;
    

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

    @Test
    public void testGetMhaReplicators() throws Exception {
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.anyString())).thenReturn(getMhaTblV2s().get(0));
        Mockito.when(replicatorGroupTblDao.queryByMhaId(Mockito.anyLong(), Mockito.anyInt())).thenReturn(getReplicatorGroupTbls().get(0));
        Mockito.when(replicatorTblDao.queryByRGroupIds(Mockito.anyList(), Mockito.anyInt())).thenReturn(getReplicatorTbls());
        Mockito.when(resourceTblDao.queryByIds(Mockito.anyList())).thenReturn(getResourceTbls());
        List<String> result = mhaServiceV2.getMhaReplicators("mha");
        Assert.assertEquals(result.size(), 1);
    }

    @Test
    public void testGetMhaAvailableResource() throws Exception {
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.anyString())).thenReturn(getMhaTblV2s().get(0));
        Mockito.when(dcTblDao.queryById(Mockito.anyLong())).thenReturn(getDcTbls().get(0));
        Mockito.when(dcTblDao.queryByRegionName(Mockito.anyString())).thenReturn(getDcTbls());
        Mockito.when(resourceTblDao.queryByDcAndType(Mockito.anyList(), Mockito.anyInt())).thenReturn(getResourceTbls());

        List<String> result = mhaServiceV2.getMhaAvailableResource("mha", ModuleEnum.APPLIER.getCode());
        Assert.assertEquals(result.size(), 1);
    }

    @Test
    public void testRecordMhaInstances() throws Exception {
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.anyString(), Mockito.anyInt())).thenReturn(getMhaTblV2s().get(0));
        Mockito.when(machineTblDao.queryByMhaId(Mockito.anyLong(), Mockito.anyInt())).thenReturn(new ArrayList<>());
        Mockito.when(machineTblDao.insert(Mockito.any(DalHints.class), Mockito.any(KeyHolder.class), Mockito.any(MachineTbl.class))).thenReturn(1);

        MhaInstanceGroupDto dto = new MhaInstanceGroupDto();
        dto.setMhaName("mha");
        MhaInstanceGroupDto.MySQLInstance mySQLInstance = new MhaInstanceGroupDto.MySQLInstance();
        mySQLInstance.setIp("127.0.0.1");
        mySQLInstance.setPort(3306);
        mySQLInstance.setUuid("uuid");
        dto.setMaster(mySQLInstance);
        Boolean isSuccess = mhaServiceV2.recordMhaInstances(dto);
        Assert.assertEquals(true, isSuccess);

        dto.setMaster(null);
        dto.setSlaves(Lists.newArrayList(mySQLInstance));
        isSuccess = mhaServiceV2.recordMhaInstances(dto);
        Assert.assertEquals(true, isSuccess);
    }

    @Test
    public void testGetMhaMessengers() throws Exception {
        String mhaName = "mha1";
        MhaTblV2 mhaTblV2 = getAllMhaData().stream().filter(e -> e.getMhaName().equals(mhaName)).findFirst().orElseThrow(() -> new Exception("data incomplete"));
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.anyString())).thenReturn(mhaTblV2);
        List<MessengerTbl> messengerTbls = JSON.parseArray("[{\"id\":1,\"messengerGroupId\":1,\"resourceId\":1,\"port\":8080,\"deleted\":0,\"createTime\":\"2028-11-02 21:41:45\",\"datachangeLasttime\":\"2015-04-01 03:25:43\"},{\"id\":2,\"messengerGroupId\":1,\"resourceId\":2,\"port\":8080,\"deleted\":0,\"createTime\":\"2028-11-02 21:41:45\",\"datachangeLasttime\":\"2015-04-01 03:25:43\"}]", MessengerTbl.class);
        Mockito.when(messengerTblDao.queryByGroupId(Mockito.anyLong())).thenReturn(messengerTbls);
        MessengerGroupTbl messengerGroupTbl = JSON.parseObject("{\"id\":1,\"mhaId\":1,\"replicatorGroupId\":1,\"deleted\":0,\"gtidExecuted\":\"gtid1\",\"datachangeLasttime\":\"2023-08-13 00:00:00\"}", MessengerGroupTbl.class);
        Mockito.when(messengerGroupTblDao.queryByMhaId(Mockito.anyLong(), Mockito.anyInt())).thenReturn(messengerGroupTbl);
        List<ResourceTbl> resourceTbls = JSON.parseArray("[{\"id\":1,\"type\":0,\"ip\":\"1.113.60.1\",\"dcId\":1,\"appId\":100023498,\"deleted\":0,\"createTime\":\"2026-06-10 15:35:59\",\"datachangeLasttime\":\"2014-07-19 15:55:18\"},{\"id\":2,\"type\":0,\"ip\":\"1.113.60.2\",\"dcId\":1,\"appId\":100023498,\"deleted\":0,\"createTime\":\"2026-06-10 15:35:59\",\"datachangeLasttime\":\"2014-07-19 15:55:18\"}]", ResourceTbl.class);
        Mockito.when(resourceTblDao.queryByIds(Mockito.anyList())).thenReturn(resourceTbls);

        List<String> mha1 = mhaServiceV2.getMhaMessengers(mhaName);

        Assert.assertEquals(2, mha1.size());
    }

    @Test
    public void testGetMhaReplicatorSlaveDelay() throws Exception {
        if (EnvUtils.fat()) {
            Mockito.when(domainConfig.getTrafficFromHickWallFat()).thenReturn("http://localhost:8080");
            Mockito.when(domainConfig.getOpsAccessTokenFat()).thenReturn("token1");
        } else {
            Mockito.when(domainConfig.getTrafficFromHickWall()).thenReturn("http://localhost:8080");
            Mockito.when(domainConfig.getOpsAccessToken()).thenReturn("token1");
        }

        Mockito.when(opsApiServiceImpl.getMhaReplicationDelay(Mockito.anyString(), Mockito.anyString())).thenReturn(getDelayInfo());
        Map<String, Long> mhaServiceV2MhaReplicatorSlaveDelay = mhaServiceV2.getMhaReplicatorSlaveDelay(
                Lists.newArrayList("mha1"));
        Assert.assertEquals(1, mhaServiceV2MhaReplicatorSlaveDelay.size());
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
    
    private List<HickWallMhaReplicationDelayEntity> getDelayInfo() {
        String json = " [\n"
                + "            {\n"
                + "                \"metric\": {\n"
                + "                    \"cluster\": \"dalCluster\",\n"
                + "                    \"address\": \"/127.0.0.1:8383\",\n"
                + "                    \"role\": \"slave\",\n"
                + "                    \"groupId\": \"21055779\",\n"
                + "                    \"ip\": \"127.0.0.1\",\n"
                + "                    \"destDc\": \"shaxy\",\n"
                + "                    \"idc\": \"SIN-AWS\",\n"
                + "                    \"srcDc\": \"sinaws\",\n"
                + "                    \"env\": \"PRO\",\n"
                + "                    \"hostname\": \"\",\n"
                + "                    \"bu\": \"BBZ\",\n"
                + "                    \"srcMha\": \"mha1\",\n"
                + "                    \"__name__\": \"fx.drc.delay_mean\",\n"
                + "                    \"appid\": \"100023928\",\n"
                + "                    \"destMha\": \"mha2\",\n"
                + "                    \"~db\": \"APM-FX\"\n"
                + "                },\n"
                + "                \"values\": [\n"
                + "                    [\n"
                + "                        1694067760,\n"
                + "                        \"68.3004539086\"\n"
                + "                    ],\n"
                + "                    [\n"
                + "                        1694067790,\n"
                + "                        \"68.2521759254\"\n"
                + "                    ],\n"
                + "                    [\n"
                + "                        1694067820,\n"
                + "                        \"68.1745746861\"\n"
                + "                    ],\n"
                + "                    [\n"
                + "                        1694067850,\n"
                + "                        \"67.9720347484\"\n"
                + "                    ],\n"
                + "                    [\n"
                + "                        1694067880,\n"
                + "                        \"67.9094651203\"\n"
                + "                    ],\n"
                + "                    [\n"
                + "                        1694067910,\n"
                + "                        \"67.8785720115\"\n"
                + "                    ],\n"
                + "                    [\n"
                + "                        1694067940,\n"
                + "                        \"67.9048681064\"\n"
                + "                    ],\n"
                + "                    [\n"
                + "                        1694067970,\n"
                + "                        \"67.9868880466\"\n"
                + "                    ],\n"
                + "                    [\n"
                + "                        1694068000,\n"
                + "                        \"67.8932267389\"\n"
                + "                    ],\n"
                + "                    [\n"
                + "                        1694068030,\n"
                + "                        \"68.0018267219\"\n"
                + "                    ],\n"
                + "                    [\n"
                + "                        1694068060,\n"
                + "                        \"68.0018267219\"\n"
                + "                    ]\n"
                + "                ]\n"
                + "            }\n"
                + "        ]";
        return JsonUtils.fromJsonToList(json,HickWallMhaReplicationDelayEntity.class);
    }

    @Test
    public void testQueryMhasWithOutDrc() throws IOException, SAXException {
        InputStream ins = FileUtils.getFileInputStream("testMeta/noARMeta.xml");
        Drc drc = DefaultSaxParser.parse(ins);
        Mockito.when(metaProviderV2.getDrc()).thenReturn(drc);
        List<String> strings = mhaServiceV2.queryMhasWithOutDrc();
        Assert.assertEquals(2, strings.size());
        Assert.assertEquals("drcTestW1", strings.get(0));
        Assert.assertEquals("drcTestW3", strings.get(1));
    }

    @Test
    public void testMarkMhaWithOutDrcOffline() throws Exception {
        InputStream ins = FileUtils.getFileInputStream("testMeta/noARMeta.xml");
        Drc drc = DefaultSaxParser.parse(ins);
        Mockito.when(metaProviderV2.getDrc()).thenReturn(drc);
        MhaTblV2 mhaTblV2 = new MhaTblV2();
        mhaTblV2.setId(1L);
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq("drcTestW3"))).thenReturn(mhaTblV2);
        Mockito.when(machineTblDao.queryByMhaId(Mockito.eq(1L),Mockito.eq(0))).thenReturn(Lists.newArrayList(new MachineTbl()));
        Mockito.when(machineTblDao.batchUpdate(Mockito.anyList())).thenReturn(new int[]{1});
        Mockito.when(mhaTblV2Dao.update(Mockito.any(MhaTblV2.class))).thenReturn(1);
        
        Pair<Boolean, Integer> res = mhaServiceV2.offlineMhasWithOutDrc(
                Lists.newArrayList("drcTestW3", "drcTestW4"));
        Assert.assertTrue(res.getKey());
        Assert.assertEquals(1, res.getValue().intValue());
    }
}