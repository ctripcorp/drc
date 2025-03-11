package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaDbMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.entity.v3.MhaDbReplicationTbl;
import com.ctrip.framework.drc.console.dto.v2.MhaDbDelayInfoDto;
import com.ctrip.framework.drc.console.dto.v3.MhaDbReplicationDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.service.v2.MhaDbMappingService;
import com.ctrip.framework.drc.console.vo.request.MhaDbQueryDto;
import com.ctrip.framework.drc.console.vo.request.MhaDbReplicationQueryDto;
import com.ctrip.framework.drc.core.http.PageResult;
import com.ctrip.framework.drc.core.meta.ReplicationTypeEnum;
import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.xpipe.tuple.Pair;
import org.apache.commons.lang.StringUtils;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class MhaDbReplicationServiceImplTest extends CommonDataInit {
    @Mock
    private MhaDbMappingService mhaDbMappingService;

    @Before
    public void setUp() throws IOException, SQLException {
        MockitoAnnotations.openMocks(this);
        super.setUp();
    }

    @Test(expected = ConsoleException.class)
    public void testOfflineMhaDbReplicationHasApplieres() throws SQLException {
        mhaDbReplicationService.offlineMhaDbReplication("mha1","mha2");
        verify(mhaDbReplicationTblDao,times(1)).batchUpdate(anyList());
    }

    @Test
    public void testOfflineMhaDbReplication() throws SQLException {
        mhaDbReplicationService.offlineMhaDbReplication("mha2","mha1");
        verify(mhaDbReplicationTblDao,times(1)).batchUpdate(anyList());
    }

    @Test
    public void testOfflineMhaDbReplicationByDbReplication1() throws SQLException {
        // still exist
        List<DbReplicationTbl> dbReplicationTbls = Lists.newArrayList(getDbReplicationTbl(2L, 4L, 0));
        mhaDbReplicationService.offlineMhaDbReplication(dbReplicationTbls);
        verify(mhaDbReplicationTblDao, times(1)).batchUpdate(anyList());
        verify(mhaDbReplicationTblDao).queryBySamples(argThat(e -> e.size() == 0));
    }

    @Test
    public void testOfflineMhaDbReplicationByDbReplication2() throws SQLException {
        // not exist
        List<DbReplicationTbl> dbReplicationTbls = Lists.newArrayList(getDbReplicationTbl(99L, 999L, 0));
        mhaDbReplicationService.offlineMhaDbReplication(dbReplicationTbls);
        verify(mhaDbReplicationTblDao, times(1)).batchUpdate(anyList());
        verify(mhaDbReplicationTblDao).queryBySamples(argThat(e -> e.size() == 1));
    }

    @Test
    public void testQueryDbReplication() {
        List<MhaDbReplicationDto> replicationDtos = mhaDbReplicationService.queryByMha("mha1", "mha2", null);
        Assert.assertFalse(CollectionUtils.isEmpty(replicationDtos));
        Assert.assertEquals("mha1", replicationDtos.get(0).getSrc().getMhaName());
        Assert.assertEquals("mha2", replicationDtos.get(0).getDst().getMhaName());
        Assert.assertFalse(StringUtils.isBlank(replicationDtos.get(0).getDst().getDbName()));
        Assert.assertNotNull(replicationDtos.get(0).getDst().getMhaDbMappingId());
        Assert.assertTrue(replicationDtos.stream().anyMatch(e->Boolean.TRUE.equals(e.getDrcStatus())));
    }
    MqType mqType = MqType.qmq;

    @Test
    public void testQueryMqReplication() {
        List<MhaDbReplicationDto> replicationDtos = mhaDbReplicationService.queryMqByMha("mha1", null, mqType);
        Assert.assertEquals(1, replicationDtos.size());
        Assert.assertEquals("mha1", replicationDtos.get(0).getSrc().getMhaName());
        Assert.assertTrue(StringUtils.isNotBlank(replicationDtos.get(0).getDbReplicationDtos().get(0).getLogicTableConfig().getDstLogicTable()));
        Assert.assertEquals(MhaDbReplicationDto.MQ_DTO, replicationDtos.get(0).getDst());
    }

    @Test
    public void testQueryByDcName() {
        List<MhaDbReplicationDto> replicationDtos = mhaDbReplicationService.queryByDcName("shaoy", null);
        Assert.assertTrue(replicationDtos.stream().anyMatch(e -> ReplicationTypeEnum.DB_TO_DB.getType().equals(e.getReplicationType()) && Boolean.TRUE.equals(e.getDrcStatus())));
        Assert.assertTrue(replicationDtos.stream().anyMatch(e -> ReplicationTypeEnum.DB_TO_MQ.getType().equals(e.getReplicationType()) && Boolean.TRUE.equals(e.getDrcStatus())));
        Assert.assertTrue(replicationDtos.stream().anyMatch(e -> ReplicationTypeEnum.DB_TO_MQ.getType().equals(e.getReplicationType()) && !Boolean.TRUE.equals(e.getDrcStatus())));
        replicationDtos.forEach(System.out::println);

        replicationDtos = mhaDbReplicationService.queryByDcName("sinaws", null);
        replicationDtos.forEach(System.out::println);
    }

    @Test
    public void testDbReplication() throws SQLException {
        List<DbReplicationTbl> dbReplicationTbls = new ArrayList<>();

        // 1. no need to maintain
        dbReplicationTbls.add(getDbReplicationTbl(1L, 3L, 0));
        dbReplicationTbls.add(getDbReplicationTbl(2L, 4L, 0));

        Pair<List<MhaDbReplicationTbl>, List<MhaDbReplicationTbl>> insertsAndUpdates = mhaDbReplicationService.getInsertsAndUpdates(dbReplicationTbls);
        List<MhaDbReplicationTbl> inserts = insertsAndUpdates.getKey();
        List<MhaDbReplicationTbl> updates = insertsAndUpdates.getValue();
        Assert.assertEquals(0, inserts.size());
        Assert.assertEquals(updates.size(), 0);


        // 2. update and inserts
        // insert
        dbReplicationTbls.add(getDbReplicationTbl(3L, 6L, 0));
        // update
        dbReplicationTbls.add(getDbReplicationTbl(1L, -1L, 1));
        dbReplicationTbls.add(getDbReplicationTbl(2L, 5L, 0));
        // exist, do nothing
        dbReplicationTbls.add(getDbReplicationTbl(2L, -1L, 1));


        insertsAndUpdates = mhaDbReplicationService.getInsertsAndUpdates(dbReplicationTbls);
        inserts = insertsAndUpdates.getKey();
        updates = insertsAndUpdates.getValue();

        Assert.assertEquals(1, inserts.size());
        Assert.assertEquals(2, updates.size());
    }

    @Test
    public void testQueryByDbNames(){
        List<MhaDbReplicationDto> replicationDtos = mhaDbReplicationService.queryByDbNames(Lists.newArrayList("db1", "db2"), ReplicationTypeEnum.DB_TO_DB);
        Assert.assertEquals(3, replicationDtos.size());
        System.out.println(replicationDtos);
    }

    @Test
    public void testQueryByDbNames2(){
        List<MhaDbReplicationDto> replicationDtos = mhaDbReplicationService.queryByDbNamesAndMhaNames(Lists.newArrayList("db1", "db2"), Lists.newArrayList("mha1"), ReplicationTypeEnum.DB_TO_DB);
        Assert.assertEquals(3, replicationDtos.size());
        System.out.println(replicationDtos);
    }

    @Test
    public void testQueryByDbNamesForMq(){
        List<MhaDbReplicationDto> replicationDtos = mhaDbReplicationService.queryByDbNames(Lists.newArrayList("db1", "db2"), ReplicationTypeEnum.DB_TO_MQ);
        Assert.assertEquals(2, replicationDtos.size());
        System.out.println(replicationDtos);
    }


    @Test
    public void testMaintainDbReplication() throws Exception {
        org.apache.commons.lang3.tuple.Pair<List<MhaDbMappingTbl>, List<MhaDbMappingTbl>> of = org.apache.commons.lang3.tuple.Pair.of(
                Lists.newArrayList(getMappingTbl(1001L, 1L, 3L)),
                Lists.newArrayList(getMappingTbl(1000L, 2L, 3L))
        );
        when(mhaDbMappingService.initMhaDbMappings(any(), any(), anyList())).thenReturn(of);

        mhaDbReplicationService.maintainMhaDbReplication("mha1", "mha2", Lists.newArrayList("db3"));
    }

    MhaDbMappingTbl getMappingTbl(Long id, Long mhaId, Long dbId) {
        MhaDbMappingTbl mappingTbl = new MhaDbMappingTbl();
        mappingTbl.setId(id);
        mappingTbl.setDbId(dbId);
        mappingTbl.setMhaId(mhaId);
        return mappingTbl;
    }

    private static DbReplicationTbl getDbReplicationTbl(Long srcId, Long dstId, int type) {
        DbReplicationTbl replication1 = new DbReplicationTbl();
        replication1.setSrcMhaDbMappingId(srcId);
        replication1.setDstMhaDbMappingId(dstId);
        replication1.setReplicationType(type);
        return replication1;
    }

    @Test
    public void testGetReplicationRelatedMha() throws SQLException {
        List<MhaTblV2> replicationRelatedMha = mhaDbReplicationService.getReplicationRelatedMha("db1", "table1");
        Assert.assertEquals(2, replicationRelatedMha.size());
        replicationRelatedMha = mhaDbReplicationService.getReplicationRelatedMha("db1", "table3");
        Assert.assertEquals(0, replicationRelatedMha.size());
        replicationRelatedMha = mhaDbReplicationService.getReplicationRelatedMha("db4", "table1");
        Assert.assertEquals(0, replicationRelatedMha.size());
    }


    @Test
    public void testQueryWithEmptyCondition() {
        MhaDbReplicationQueryDto queryDto = new MhaDbReplicationQueryDto();
        PageResult<MhaDbReplicationDto> query = mhaDbReplicationService.query(queryDto);
        verify(mhaDbMappingService, never()).query(any());
        Assert.assertNotEquals(0, query.getData().size());
    }

    @Test
    public void testQuery() {
        when(mhaDbMappingService.query(any())).thenReturn(getMhaDbMappingTbls());
        MhaDbReplicationQueryDto queryDto = new MhaDbReplicationQueryDto();
        queryDto.setSrcMhaDb(getMhaDbQueryDto());
        queryDto.setDstMhaDb(getMhaDbQueryDto());
        queryDto.setRelatedMhaDb(getMhaDbQueryDto());
        queryDto.setDrcStatus(BooleanEnum.TRUE.getCode());
        PageResult<MhaDbReplicationDto> query = mhaDbReplicationService.query(queryDto);
        System.out.println("query = " + query);
    }

    @Test
    public void testQueryDelay() {
        List<Long> longs = Lists.newArrayList(1L);
        when(mysqlServiceV2.getDbDelayUpdateTime(eq("mha1"), eq("mha1"), anyList())).thenReturn(getTimeMap("db1", 120L));
        when(mysqlServiceV2.getDbDelayUpdateTime(eq("mha1"), eq("mha2"), anyList())).thenReturn(getTimeMap("db1", 100L));
        List<MhaDbDelayInfoDto> replicationDelays = mhaDbReplicationService.getReplicationDelays(longs);
        Assert.assertEquals((long) 20L, (long) replicationDelays.get(0).getDelay());
    }

    private static HashMap<String, Long> getTimeMap(String db, Long time) {
        HashMap<String, Long> map = new HashMap<>();
        map.put(db, time);
        return map;
    }

    private static MhaDbQueryDto getMhaDbQueryDto() {
        MhaDbQueryDto srcMhaDb = new MhaDbQueryDto();
        srcMhaDb.setRegionId(1L);
        return srcMhaDb;
    }

    private static ArrayList<MhaDbMappingTbl> getMhaDbMappingTbls() {
        MhaDbMappingTbl mhaDbMappingTbl = new MhaDbMappingTbl();
        mhaDbMappingTbl.setId(1L);
        return Lists.newArrayList(mhaDbMappingTbl);
    }
}

