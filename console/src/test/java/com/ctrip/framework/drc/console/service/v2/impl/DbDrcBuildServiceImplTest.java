package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.entity.v2.ApplierGroupTblV2;
import com.ctrip.framework.drc.console.dto.v3.DbApplierDto;
import com.ctrip.framework.drc.console.dto.v3.MhaDbDto;
import com.ctrip.framework.drc.console.dto.v3.MhaDbReplicationDto;
import com.ctrip.framework.drc.console.param.v2.DrcBuildBaseParam;
import com.ctrip.framework.drc.console.param.v2.DrcBuildParam;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.MhaDbReplicationService;
import com.ctrip.framework.drc.core.entity.Drc;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class DbDrcBuildServiceImplTest extends CommonDataInit {

    @Mock
    private MhaDbReplicationService mhaDbReplicationService;

    @Mock
    private MetaInfoServiceV2 metaInfoService;

    @Before
    public void setUp() throws IOException, SQLException {
        MockitoAnnotations.openMocks(this);
        MhaDbReplicationDto dto1 = new MhaDbReplicationDto();
        String srcMha = "mha1";
        String dstMha = "mha2";

        dto1.setId(1L);
        dto1.setSrc(new MhaDbDto(1L, srcMha, "db1"));
        dto1.setDst(new MhaDbDto(3L, srcMha, "db1"));
        dto1.setReplicationType(0);

        MhaDbReplicationDto dto2 = new MhaDbReplicationDto();
        dto2.setId(2L);
        dto2.setSrc(new MhaDbDto(2L, srcMha, "db2"));
        dto2.setDst(new MhaDbDto(4L, srcMha, "db2"));
        dto2.setReplicationType(0);

        List<MhaDbReplicationDto> mhaDbReplicationDtos = Lists.newArrayList(dto1, dto2);
        when(mhaDbReplicationService.queryByMha(eq(srcMha), eq(dstMha), any())).thenReturn(mhaDbReplicationDtos);

        // messenger
        MhaDbReplicationDto dto3 = new MhaDbReplicationDto();
        dto3.setId(1L);
        dto3.setSrc(new MhaDbDto(1L, srcMha, "db1"));
        dto3.setDst(MhaDbReplicationDto.MQ_DTO);
        dto3.setReplicationType(1);
        MhaDbReplicationDto dto4 = new MhaDbReplicationDto();
        dto4.setId(2L);
        dto4.setSrc(new MhaDbDto(2L, srcMha, "db2"));
        dto4.setDst(MhaDbReplicationDto.MQ_DTO);
        dto4.setReplicationType(1);

        when(mhaDbReplicationService.queryMqByMha(eq(srcMha), any())).thenReturn(Lists.newArrayList(dto3, dto4));

        super.setUp();
    }

    @Test
    public void testGetAppliers() throws Exception {
        String srcMha = "mha1";
        String dstMha = "mha2";
        List<DbApplierDto> dbAppliers = dbDrcBuildService.getMhaDbAppliers(srcMha, dstMha);
        Assert.assertFalse(CollectionUtils.isEmpty(dbAppliers));
    }

    @Test
    public void testGetMessenger() throws Exception {
        String srcMha = "mha1";
        List<DbApplierDto> dbAppliers = dbDrcBuildService.getMhaDbMessengers(srcMha);
        Assert.assertFalse(CollectionUtils.isEmpty(dbAppliers));
        Assert.assertFalse(CollectionUtils.isEmpty(dbAppliers.get(0).getIps()));
        Assert.assertFalse(StringUtils.isEmpty(dbAppliers.get(0).getGtidInit()));
    }

    @Test
    public void testConfigurable() {
        when(defaultConsoleConfig.getDbApplierConfigureSwitch(anyString())).thenReturn(true);
        Assert.assertTrue(dbDrcBuildService.isDbApplierConfigurable("mha"));
    }

    @Test
    public void testBuildAppliers() throws Exception {

        DrcBuildParam drcBuildParam = new DrcBuildParam();
        DrcBuildBaseParam srcBuildParam = new DrcBuildBaseParam();
        srcBuildParam.setMhaName("mha1");
        drcBuildParam.setSrcBuildParam(srcBuildParam);
        DrcBuildBaseParam dstBuildParam = new DrcBuildBaseParam();
        dstBuildParam.setApplierInitGtid("a:123");
        dstBuildParam.setMhaName("mha2");
        dstBuildParam.setDbApplierDtos(Lists.newArrayList(new DbApplierDto(Lists.newArrayList("2.113.60.2"), "gtidInit1", "db2")));
        drcBuildParam.setDstBuildParam(dstBuildParam);
        when(defaultConsoleConfig.getDbApplierConfigureSwitch(anyString())).thenReturn(true);
        when(metaInfoService.getDrcReplicationConfig(anyString(), anyString())).thenReturn(new Drc());
        dbDrcBuildService.buildDbApplier(drcBuildParam);

        verify(applierTblV3Dao, times(1)).batchInsert(anyList());
        verify(applierTblV3Dao, times(1)).batchUpdate(anyList());

        verify(applierGroupTblV3Dao, times(1)).batchInsertWithReturnId(anyList());
        verify(applierGroupTblV3Dao, times(1)).batchUpdate(anyList());

        verify(applierTblV2Dao, never()).insert(anyList());
        verify(applierGroupTblV2Dao, times(1)).update(any(ApplierGroupTblV2.class));
        verify(applierGroupTblV2Dao, never()).insertOrReCover(any(), anyString());
    }


    @Test
    public void testBuildMessengers() throws Exception {

        DrcBuildBaseParam dstBuildParam = new DrcBuildBaseParam();
        dstBuildParam.setMhaName("mha1");
        dstBuildParam.setDbApplierDtos(Lists.newArrayList(new DbApplierDto(Lists.newArrayList("ip1", "ip2"), "gtidInit1", "db1"), new DbApplierDto(Lists.newArrayList("ip1"), "gtidInit2", "db2")));
        when(defaultConsoleConfig.getDbApplierConfigureSwitch(anyString())).thenReturn(true);
        when(metaInfoService.getDrcMessengerConfig(anyString())).thenReturn(new Drc());
        dbDrcBuildService.buildDbMessenger(dstBuildParam);

        verify(messengerTblV3Dao, times(1)).batchInsert(anyList());
        verify(messengerTblV3Dao, times(1)).batchUpdate(anyList());

        verify(messengerGroupTblV3Dao, times(1)).upsert(anyList());

        verify(messengerTblDao, never()).insert(anyList());
        verify(messengerGroupTblDao, never()).upsertIfNotExist(any(), any(), any());
    }

    @Test
    public void testGetMhaInitGtidWhenRollbackFromDbApply() throws SQLException {
        HashMap<String, String> map = Maps.newHashMap();
        map.put("db1", "a:1-150,abc:1-100");
        map.put("db2", "abc:1-10");
        when(mysqlServiceV2.getMhaDbAppliedGtid(anyString())).thenReturn(map);
        String gtid = dbDrcBuildService.getDbDrcExecutedGtidTruncate("mha1", "mha2");
        Assert.assertEquals("a:1-110,abc:1-10", gtid);
    }

    @Test
    public void testGetDbAppliersInitGtid() throws SQLException {
        HashMap<String, String> map = Maps.newHashMap();
        when(mysqlServiceV2.getMhaAppliedGtid(anyString())).thenReturn("a:1-150,b:1-20");
        String gtid = dbDrcBuildService.getMhaDrcExecutedGtidTruncate("mha1", "mha2");
        Assert.assertEquals("a:1-150,b:1-20", gtid);
    }
}
