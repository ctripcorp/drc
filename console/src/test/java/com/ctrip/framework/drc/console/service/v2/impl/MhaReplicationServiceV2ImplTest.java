package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dto.v2.MhaDelayInfoDto;
import com.ctrip.framework.drc.console.dto.v2.MhaReplicationDto;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.param.v2.MhaReplicationQuery;
import com.ctrip.framework.drc.core.http.PageResult;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class MhaReplicationServiceV2ImplTest extends CommonDataInit {


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
        
        
        // todo hdpan  
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
        verify(mhaTblV2Dao, Mockito.times(1)).update(Mockito.anyList());
        
        releaseMockConfig();
    }
}