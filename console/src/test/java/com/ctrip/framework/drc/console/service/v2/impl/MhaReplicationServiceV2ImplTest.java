package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dto.v2.MhaDelayInfoDto;
import com.ctrip.framework.drc.console.dto.v2.MhaReplicationDto;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.param.v2.MhaReplicationQuery;
import com.ctrip.framework.drc.console.service.v2.MhaDbReplicationService;
import com.ctrip.framework.drc.console.service.v2.MhaServiceV2;
import com.ctrip.framework.drc.console.service.v2.PojoBuilder;
import com.ctrip.framework.drc.console.vo.v2.MhaApplierOfflineView;
import com.ctrip.framework.drc.console.vo.v2.MhaSyncView;
import com.ctrip.framework.drc.core.http.PageResult;
import com.ctrip.xpipe.tuple.Pair;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


public class MhaReplicationServiceV2ImplTest extends CommonDataInit {


    @Mock
    MhaDbReplicationService mhaDbReplicationService;
    @Mock
    MhaServiceV2 mhaServiceV2;


    @Before
    public void setUp() throws SQLException, IOException {
        MockitoAnnotations.openMocks(this);
        super.setUp();
    }

    @Test
    public void testQueryByPage() {
        PageResult<MhaReplicationTbl> pageResult = mhaReplicationServiceV2.queryByPage(new MhaReplicationQuery());

        Assert.assertNotNull(pageResult);

        Assert.assertFalse(CollectionUtils.isEmpty(pageResult.getData()));
        Assert.assertNotEquals(0, pageResult.getTotalCount());
    }

    @Test
    public void testQueryRelatedReplications() {
        List<MhaReplicationTbl> mhaReplicationTbls = mhaReplicationServiceV2.queryRelatedReplications(Lists.newArrayList(1L));

        Assert.assertNotNull(mhaReplicationTbls);
        Assert.assertFalse(CollectionUtils.isEmpty(mhaReplicationTbls));

    }

    @Test
    public void testQueryRelatedReplicationsByDbNames() {

        List<String> dbNames = Lists.newArrayList("db1", "db2");
        String mhaName = "mha1";
        List<String> mhaNames = Lists.newArrayList(mhaName);
        List<MhaReplicationDto> mhaReplicationDtos = mhaReplicationServiceV2.queryRelatedReplications(mhaNames, dbNames);

        Assert.assertEquals(3, mhaReplicationDtos.size());
        for (MhaReplicationDto replicationDto : mhaReplicationDtos) {
            Assert.assertTrue(replicationDto.getSrcMha().getName().equals(mhaName) || replicationDto.getDstMha().getName().equals(mhaName));
            Assert.assertNotEquals(0, replicationDto.getDbs().size());
            Assert.assertTrue(dbNames.containsAll(replicationDto.getDbs()));
        }
    }

    @Test
    public void testGetMhaReplicationSingleDelay() {
        String mha1 = "mha1";
        String mha2 = "mha2";
        long srcNowTime = 205L;
        long dstLastUpdateTime = 100L;
        when(mysqlServiceV2.getDelayUpdateTime(mha1, mha2)).thenReturn(dstLastUpdateTime);
        when(mysqlServiceV2.getCurrentTime(mha1)).thenReturn(srcNowTime);
        MhaDelayInfoDto delay = mhaReplicationServiceV2.getMhaReplicationDelay(mha1, mha2);

        Assert.assertNotNull(delay);
        Assert.assertEquals(mha1, delay.getSrcMha());
        Assert.assertEquals(mha2, delay.getDstMha());
        long delayExpected = srcNowTime - dstLastUpdateTime;
        Assert.assertEquals(delayExpected, delay.getDelay().longValue());
        System.out.println(delay);
    }

    @Test
    public void testGetMhaReplicationDelays() {
        String mha1 = "mha1";
        String mha2 = "mha2";
        String mha3 = "mha3";
        long srcNowTime = 205L;
        long mha1To2UpdateTIme = 50L;
        long mha1To3UpdateTIme = 100L;
        List<MhaReplicationDto> list = new ArrayList<>();


        when(mysqlServiceV2.getCurrentTime(mha1)).thenReturn(srcNowTime);
        list.add(MhaReplicationDto.from("mha1", "mha2"));
        when(mysqlServiceV2.getDelayUpdateTime(mha1, mha2)).thenReturn(mha1To2UpdateTIme);
        list.add(MhaReplicationDto.from("mha1", "mha3"));
        when(mysqlServiceV2.getDelayUpdateTime(mha1, mha3)).thenReturn(mha1To3UpdateTIme);

        List<MhaDelayInfoDto> delay = mhaReplicationServiceV2.getMhaReplicationDelays(list);

        for (MhaDelayInfoDto infoDTO : delay) {
            Long expectDelay = null;
            if (infoDTO.getSrcMha().equals(mha1) && infoDTO.getDstMha().equals(mha2)) {
                expectDelay = srcNowTime - mha1To2UpdateTIme;
            } else if (infoDTO.getSrcMha().equals(mha1) && infoDTO.getDstMha().equals(mha3)) {
                expectDelay = srcNowTime - mha1To3UpdateTIme;
            }
            Assert.assertEquals(expectDelay, infoDTO.getDelay());
        }
        Assert.assertNotNull(delay);
        Assert.assertEquals(list.size(), delay.size());
        System.out.println(delay);
    }

    @Test
    public void testGetMhaReplicationDelaysV2() {
        String mha1 = "mha1";
        String mha2 = "mha2";
        String mha3 = "mha3";
        long srcNowTime = 205L;
        long mha1To2UpdateTIme = 50L;
        long mha1To3UpdateTIme = 100L;
        List<MhaReplicationDto> list = new ArrayList<>();


        when(mysqlServiceV2.getCurrentTime(mha1)).thenReturn(srcNowTime);
        list.add(MhaReplicationDto.from("mha1", "mha2"));
        when(mysqlServiceV2.getDelayUpdateTime(mha1, mha2)).thenReturn(mha1To2UpdateTIme);
        list.add(MhaReplicationDto.from("mha1", "mha3"));
        when(mysqlServiceV2.getDelayUpdateTime(mha1, mha3)).thenReturn(mha1To3UpdateTIme);

        List<MhaDelayInfoDto> delay = mhaReplicationServiceV2.getMhaReplicationDelaysV2(list);

        for (MhaDelayInfoDto infoDTO : delay) {
            Long expectDelay = null;
            if (infoDTO.getSrcMha().equals(mha1) && infoDTO.getDstMha().equals(mha2)) {
                expectDelay = srcNowTime - mha1To2UpdateTIme;
            } else if (infoDTO.getSrcMha().equals(mha1) && infoDTO.getDstMha().equals(mha3)) {
                expectDelay = srcNowTime - mha1To3UpdateTIme;
            }
            Assert.assertEquals(expectDelay, infoDTO.getDelay());
        }
        Assert.assertNotNull(delay);
        Assert.assertEquals(list.size(), delay.size());
        System.out.println(delay);
    }

    @Test
    public void testDeleteMhaReplication() throws SQLException, IOException {
//        Mockito.when(mhaTblV2Dao.queryByPk(Mockito.eq(1L))).thenReturn();
//        Mockito.when(mhaTblV2Dao.queryByPk(Mockito.eq(2L))).thenReturn(null);
//        Mockito.when(mhaDbMappingTblDao.queryByMhaId(Mockito.eq(1L))).thenReturn(null);
//        Mockito.when(dbReplicationTblDao.queryBySrcMappingIds(Mockito.eq(Lists.newArrayList(1L)),Mockito.eq(ReplicationTypeEnum.DB_TO_DB.getType()))).thenReturn(null);
//        Mockito.when(applierGroupTblV2Dao.queryByMhaReplicationId()).thenReturn(null);
//        Mockito.when(applierTblV2Dao.queryByApplierGroupId()).thenReturn(null);
//        Mockito.when(messengerGroupTblDao.queryByMhaId()).thenReturn(null);
//        Mockito.when(replicatorGroupTblDao.queryByMhaId()).thenReturn(null);
//        Mockito.when(replicatorTblDao.queryByRGroupIds()).thenReturn(null);
//        Mockito.when(mhaReplicationTblDao.queryByRelatedMhaId()).thenReturn(null);
//        Mockito.when(dbReplicationTblDao.queryBySrcMappingIds(Mockito.eq(Lists.newArrayList(1L)),Mockito.eq(ReplicationTypeEnum.DB_TO_DB.getType()))).thenReturn(null);
        // change default mock data;
        reMock("/testData/mhaReplicationDeleteCase/");
        // mock update
        Mockito.when(mhaReplicationTblDao.update(Mockito.any(MhaReplicationTbl.class))).thenReturn(1);
        Mockito.when(mhaTblV2Dao.update(Mockito.anyList())).thenReturn(new int[]{1});
        Mockito.when(mhaServiceV2.offlineMha(Mockito.anyString())).thenReturn(true);

        // case 1 dbReplication not empty 
        try {
            boolean b = mhaReplicationServiceV2.deleteMhaReplication(1L);
        } catch (ConsoleException e) {
            Assert.assertTrue(e.getMessage().contains("DbReplications not empty!"));
        }
        // case 2 applier not empty
        try {
            boolean b = mhaReplicationServiceV2.deleteMhaReplication(2L);
        } catch (ConsoleException e) {
            Assert.assertTrue(e.getMessage().contains(":Applier not empty"));
        }
        // case 3 messenger not empty, not delete mha  mha3
        // case 4 messenger empty, delete mha but replicator not empty mha4
        try {
            boolean b = mhaReplicationServiceV2.deleteMhaReplication(3L);
        } catch (ConsoleException e) {
            Assert.assertTrue(e.getMessage().contains("Replicator not empty!"));
        }

        // case 5 messenger empty, delete mha and replicator empty
        // delete mha6 , mha5 has other replication

        boolean b = mhaReplicationServiceV2.deleteMhaReplication(4L);
        verify(mhaReplicationTblDao, Mockito.times(1)).update(Mockito.any(MhaReplicationTbl.class));
        verify(mhaServiceV2, Mockito.times(1)).offlineMha(Mockito.eq("mha6"));

        releaseMockConfig();
    }


    @Test
    public void testParseQconfig() {
        String input = getQConfigTemplate1();
        Map<String, String> expected = new HashMap<>();
        expected.put("bbzimelongdb_dalcluster.bbzimelong.bbzimelongshali.purgedgtid", "ec4b75e5-2a12-11eb-a3d3-506b4b47803c:1-589347286,ece065e2-454c-11eb-bd4c-506b4b4776ec:1-79225977");
        expected.put("bbzimelongdb_dalcluster.itcchatos7.bbzimelongshali.purgedgtid", "9382d1a9-b482-11eb-8957-94292f70c4d7:158736104-444988348,95a31a81-070c-11ec-80d4-b8cef6507324:26941-210088954,ec4b75e5-2a12-11eb-a3d3-506b4b47803c:1-619973813,ece065e2-454c-11eb-bd4c-506b4b4776ec:1-79225977");
        expected.put("bbzimelongdetailshardbasedb_dalcluster.bbzimelongdetailshard.bbzimelongdetailshardshali.purgedgtid", "935066db-454b-11eb-bcfe-506b4b2af01e:1-271601355,c4fff537-2a2a-11eb-aae0-506b4b4791b4:1-1806187800");
        expected.put("bbzimelongdetailshardbasedb_dalcluster.bbzimelongdetailshard03os7.bbzimelongdetailshardshali.purgedgtid", "935066db-454b-11eb-bcfe-506b4b2af01e:1-271601355,c4fff537-2a2a-11eb-aae0-506b4b4791b4:1-1806187800");


        Map<String, String> output = mhaReplicationServiceV2.parseConfigFileGtidContent(input);
        Assert.assertEquals(expected, output);
    }

    private static String getQConfigTemplate1() {
        String input = "conflict.log.upload.url=http://drc.ctripcorp.com/api/drc/v2/log/conflict/\n" +
                "conflict.log.upload.switch=on\n" +
                "# 本端clusterId.对端mha.purgedgtid=对端gtidset\n" +
                "bbzimelongdb_dalcluster.bbzimelong.bbzimelongshali.purgedgtid=ec4b75e5-2a12-11eb-a3d3-506b4b47803c:1-589347286,ece065e2-454c-11eb-bd4c-506b4b4776ec:1-79225977\n" +
                "bbzimelongdb_dalcluster.itcchatos7.bbzimelongshali.purgedgtid=9382d1a9-b482-11eb-8957-94292f70c4d7:158736104-444988348,95a31a81-070c-11ec-80d4-b8cef6507324:26941-210088954,ec4b75e5-2a12-11eb-a3d3-506b4b47803c:1-619973813,ece065e2-454c-11eb-bd4c-506b4b4776ec:1-79225977\n" +
                "bbzimelongdetailshardbasedb_dalcluster.bbzimelongdetailshard.bbzimelongdetailshardshali.purgedgtid=935066db-454b-11eb-bcfe-506b4b2af01e:1-271601355,c4fff537-2a2a-11eb-aae0-506b4b4791b4:1-1806187800\n" +
                "bbzimelongdetailshardbasedb_dalcluster.bbzimelongdetailshard03os7.bbzimelongdetailshardshali.purgedgtid=935066db-454b-11eb-bcfe-506b4b2af01e:1-271601355,c4fff537-2a2a-11eb-aae0-506b4b4791b4:1-1806187800\n" +
                "bbzimelongdetailshardbasedb_dalcluster.invalid.gtid.purgedgtid=\n";
        return input;
    }


    @Test
    public void testSynConfigGtidAlreadyExistException() {
        String input = "mha_dalcluster.mha2.mha1.purgedgtid=ec4b75e5-2a12-11eb-a3d3-506b4b47803c:1-589347286,ece065e2-454c-11eb-bd4c-506b4b4776ec:1-79225977\n";
        Pair<Integer, List<String>> result = mhaReplicationServiceV2.synApplierGtidInfoFromQConfig(input, false);
        Integer count = result.getKey();
        List<String> messages = result.getValue();
        Assert.assertEquals(1, count.intValue());
        Assert.assertEquals(1, messages.size());
    }

    @Test
    public void testSynConfigMhaNotExist() {
        String input = "mha_dalcluster.mha2NotExist.mha1.purgedgtid=ec4b75e5-2a12-11eb-a3d3-506b4b47803c:1-589347286,ece065e2-454c-11eb-bd4c-506b4b4776ec:1-79225977\n";
        Pair<Integer, List<String>> result = mhaReplicationServiceV2.synApplierGtidInfoFromQConfig(input, false);
        Integer count = result.getKey();
        List<String> messages = result.getValue();
        Assert.assertEquals(0, count.intValue());
        Assert.assertTrue(messages.size() > 0);
    }

    @Test
    public void testSynConfigMhaReplicationNotExist() {
        String input = "mha_dalcluster.mha2.mha3.purgedgtid=ec4b75e5-2a12-11eb-a3d3-506b4b47803c:1-589347286,ece065e2-454c-11eb-bd4c-506b4b4776ec:1-79225977\n";
        Pair<Integer, List<String>> result = mhaReplicationServiceV2.synApplierGtidInfoFromQConfig(input, false);
        Integer count = result.getKey();
        List<String> messages = result.getValue();
        Assert.assertEquals(0, count.intValue());
        Assert.assertTrue(messages.size() > 0);
    }

    @Test
    public void testSynConfi2g() {
        String input = "mha_dalcluster.mha3.mha1.purgedgtid=ec4b75e5-2a12-11eb-a3d3-506b4b47803c:1-589347286,ece065e2-454c-11eb-bd4c-506b4b4776ec:1-79225977\n";
        Pair<Integer, List<String>> result = mhaReplicationServiceV2.synApplierGtidInfoFromQConfig(input, false);
        Integer count = result.getKey();
        List<String> messages = result.getValue();
        Assert.assertEquals(1, count.intValue());
        Assert.assertEquals(0, messages.size());
    }

    @Test
    public void test() {
        List<MhaReplicationTbl> mhaReplicationTbls = mhaReplicationServiceV2.queryAllHasActiveMhaDbReplications();
        Assert.assertFalse(CollectionUtils.isEmpty(mhaReplicationTbls));
    }

    @Test
    public void testMhaSyncCount() throws Exception {
        Mockito.when(applierTblV3Dao.queryAllExist()).thenReturn(PojoBuilder.getApplierTblV3s());
        Mockito.when(applierGroupTblV3Dao.queryAllExist()).thenReturn(PojoBuilder.getApplierGroupTblV3s());
        Mockito.when(dbReplicationTblDao.queryAllExist()).thenReturn(PojoBuilder.getDbReplicationTbls());
        Mockito.when(mhaDbMappingTblDao.queryAllExist()).thenReturn(PojoBuilder.getMhaDbMappingTbls1());
        Mockito.when(mhaReplicationTblDao.queryAllExist()).thenReturn(PojoBuilder.getMhaReplicationTbls());
        Mockito.when(mhaDbReplicationTblDao.queryAllExist()).thenReturn(PojoBuilder.getMhaDbReplicationTbls());
        Mockito.when(dbTblDao.queryAllExist()).thenReturn(PojoBuilder.getDbTbls());
        Mockito.when(mhaTblV2Dao.queryAllExist()).thenReturn(PojoBuilder.getMhaTblV2s());
        Mockito.when(messengerTblDao.queryAllExist()).thenReturn(PojoBuilder.getMessengers());
        Mockito.when(messengerGroupTblDao.queryAllExist()).thenReturn(PojoBuilder.getMessengerGroups());

        MhaSyncView result = mhaReplicationServiceV2.mhaSyncCount();

        Assert.assertEquals(result.getMhaSyncIds().size(), 1);
        Assert.assertEquals(result.getDbNameSet().size(), 1);
        Assert.assertEquals(result.getDbSyncSet().size(), 1);
        Assert.assertEquals(result.getDalClusterSet().size(), 1);
        Assert.assertEquals(result.getDbMessengerSet().size(), 1);
        Assert.assertEquals(result.getDbOtterSet().size(), 1);

    }

    @Test
    public void testGetOfflineApplier() throws SQLException {
        MhaApplierOfflineView res = mhaReplicationServiceV2.getMhaApplierShouldOffline();
        Assert.assertNotNull(res);
        Assert.assertEquals(2, res.getMhaReplicationWithMhaApplierCount());
        Assert.assertEquals(1, res.getMhaReplicationWithDbApplierCount());
        Assert.assertEquals(res.getMhaReplicationWithBothMhaApplierAndDbApplierIds().size(), res.getMhaReplicationWithBothMhaApplierAndDbApplierCount());
    }

    @Test
    public void testOfflineApplier() throws SQLException {
        int count = mhaReplicationServiceV2.offlineMhaAppliers();
        Assert.assertEquals(2, count);
        verify(applierTblV2Dao,times(1)).batchUpdate(any());
    }
}