package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplierGroupTblV2;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dto.v2.MqConfigDto;
import com.ctrip.framework.drc.console.dto.v3.*;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.param.v2.*;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.*;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.DbClusterInfoDto;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.console.vo.check.v2.MqConfigCheckVo;
import com.ctrip.framework.drc.console.vo.v2.ResourceView;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class DbDrcBuildServiceImplTest extends CommonDataInit {

    @Mock
    private MhaDbReplicationService mhaDbReplicationService;
    @Mock
    private MetaInfoServiceV2 metaInfoService;
    @Mock
    private ResourceService resourceService;
    @Mock
    private MhaServiceV2 mhaServiceV2;
    @Mock
    private DbaApiService dbaApiService;
    @Mock
    private DrcAutoBuildService drcAutoBuildService;
    @Mock
    private DrcBuildServiceV2 drcBuildServiceV2;
    @Mock
    private MessengerServiceV2Impl messengerServiceV2;

    @Before
    public void setUp() throws IOException, SQLException {
        MockitoAnnotations.openMocks(this);
        MhaDbReplicationDto dto1 = new MhaDbReplicationDto();
        String srcMha = "mha1";
        String dstMha = "mha2";
        DbTbl dbTbl1 = new DbTbl();
        dbTbl1.setDbName("db1");
        DbTbl dbTbl2 = new DbTbl();
        dbTbl2.setDbName("db2");
        DbTbl dbTbl3 = new DbTbl();
        dbTbl3.setDbName("db3");
        DbTbl dbTbl4 = new DbTbl();
        dbTbl4.setDbName("db4");

        DcDo dcDo1 = new DcDo();
        dcDo1.setRegionName("src1");
        DcDo dcDo2 = new DcDo();
        dcDo2.setRegionName("dst1");


        MhaTblV2 srcMhaTbl = getMhaTblV2("mha1");
        MhaTblV2 dstMhaTbl = getMhaTblV2("mha2");
        LogicTableConfig config = new LogicTableConfig();
        config.setLogicTable("old_table");


        dto1.setId(1L);
        dto1.setSrc(MhaDbDto.from(1L, srcMhaTbl, dbTbl1, dcDo1));
        dto1.setDst(MhaDbDto.from(3L, dstMhaTbl, dbTbl1, dcDo2));
        dto1.setReplicationType(0);
        dto1.setDbReplicationDtos(Lists.newArrayList(new DbReplicationDto(1001L, config)));

        MhaDbReplicationDto dto2 = new MhaDbReplicationDto();
        dto2.setId(2L);
        dto2.setSrc(MhaDbDto.from(2L, srcMhaTbl, dbTbl2, dcDo1));
        dto2.setDst(MhaDbDto.from(4L, dstMhaTbl, dbTbl2, dcDo2));
        dto2.setDbReplicationDtos(Lists.newArrayList(new DbReplicationDto(1002L, config)));
        dto2.setReplicationType(0);

        MhaDbReplicationDto dto11 = new MhaDbReplicationDto();
        dto11.setId(3L);
        dto11.setSrc(MhaDbDto.from(5L, srcMhaTbl, dbTbl3, dcDo1));
        dto11.setDst(MhaDbDto.from(6L, dstMhaTbl, dbTbl3, dcDo2));
        dto11.setDbReplicationDtos(Lists.newArrayList(new DbReplicationDto(1003L, config)));
        dto11.setReplicationType(0);


        MhaDbReplicationDto dto22 = new MhaDbReplicationDto();
        dto22.setId(4L);
        dto22.setSrc(MhaDbDto.from(7L, srcMhaTbl, dbTbl4, dcDo1));
        dto22.setDst(MhaDbDto.from(8L, dstMhaTbl, dbTbl4, dcDo2));
        dto22.setDbReplicationDtos(Lists.newArrayList(new DbReplicationDto(1004L, config)));
        dto22.setReplicationType(0);

        List<MhaDbReplicationDto> mhaDbReplicationDtos = Lists.newArrayList(dto1, dto2, dto11, dto22);

        LogicTableConfig mqConfig = new LogicTableConfig();
        mqConfig.setLogicTable("old_table");
        mqConfig.setDstLogicTable("bbz.test.binlog");
        mqConfig.setMessengerFilterId(1L);

        MhaDbReplicationDto dtoForMq1 = new MhaDbReplicationDto();
        dtoForMq1.setId(1L);
        dtoForMq1.setSrc(MhaDbDto.from(1L, srcMhaTbl, dbTbl1, dcDo1));
        dtoForMq1.setDst(MhaDbReplicationDto.MQ_DTO);
        dtoForMq1.setDbReplicationDtos(Lists.newArrayList(new DbReplicationDto(1005L, mqConfig)));
        dtoForMq1.setReplicationType(1);


        MhaDbReplicationDto dtoForMq2 = new MhaDbReplicationDto();
        dtoForMq2.setId(2L);
        dtoForMq2.setSrc(MhaDbDto.from(2L, srcMhaTbl, dbTbl2, dcDo1));
        dtoForMq2.setDst(MhaDbReplicationDto.MQ_DTO);
        dtoForMq2.setDbReplicationDtos(Lists.newArrayList(new DbReplicationDto(1006L, mqConfig)));
        dtoForMq2.setReplicationType(1);

        List<MhaDbReplicationDto> mhaDbReplicationDtosForMq = Lists.newArrayList(dtoForMq1, dtoForMq2);


        when(mhaDbReplicationService.queryByMha(eq(srcMha), eq(dstMha), any())).thenAnswer(param -> {
            List<String> dbNames = param.getArgument(2);
            return mhaDbReplicationDtos.stream().filter(e -> {
                if (dbNames == null) {
                    return true;
                }
                return dbNames.contains(e.getSrc().getDbName());
            }).collect(Collectors.toList());
        });
        when(mhaDbReplicationService.queryByDbNames(anyList(), eq(ReplicationTypeEnum.DB_TO_DB))).thenAnswer(param -> {
            List<String> dbNames = param.getArgument(0);
            return mhaDbReplicationDtos.stream().filter(e -> dbNames.contains(e.getSrc().getDbName())).collect(Collectors.toList());
        });

        when(mhaDbReplicationService.queryByDbNames(anyList(), eq(ReplicationTypeEnum.DB_TO_MQ))).thenAnswer(param -> {
            List<String> dbNames = param.getArgument(0);
            return mhaDbReplicationDtosForMq.stream().filter(e -> dbNames.contains(e.getSrc().getDbName())).collect(Collectors.toList());
        });

        // messenger
        MhaDbReplicationDto dto3 = new MhaDbReplicationDto();
        dto3.setId(1L);
        dto3.setSrc(new MhaDbDto(1L, srcMha, "db1"));
        dto3.setDst(MhaDbReplicationDto.MQ_DTO);
        dto3.setReplicationType(1);
        dto3.setDbReplicationDtos(Lists.newArrayList(new DbReplicationDto(1001L, config)));

        MhaDbReplicationDto dto4 = new MhaDbReplicationDto();
        dto4.setId(2L);
        dto4.setSrc(new MhaDbDto(2L, srcMha, "db2"));
        dto4.setDst(MhaDbReplicationDto.MQ_DTO);
        dto4.setDbReplicationDtos(Lists.newArrayList(new DbReplicationDto(1001L, config)));
        dto4.setReplicationType(1);

        when(mhaDbReplicationService.queryMqByMha(eq(srcMha), any())).thenReturn(Lists.newArrayList(dto3, dto4));
        when(metaInfoService.getDrcReplicationConfig(anyString(), anyString())).thenReturn(new Drc());
        String json2 = "[{\"clusterList\":[{\"clusterName\":\"mha1\",\"nodes\":[{\"instancePort\":55111,\"instanceZoneId\":\"NTGXH\",\"role\":\"master\",\"ipBusiness\":\"11.11.11.1\"},{\"instancePort\":55111,\"instanceZoneId\":\"NTGXH\",\"role\":\"slave\",\"ipBusiness\":\"11.11.11.2\"}],\"env\":\"fat\",\"zoneId\":\"NTGXH\"},{\"clusterName\":\"sin1\",\"nodes\":[{\"instancePort\":55111,\"instanceZoneId\":\"sin-aws\",\"role\":\"master\",\"ipBusiness\":\"sin.rds.amazonaws.com\"}],\"env\":\"fat\",\"zoneId\":\"sin-aws\"}],\"dbName\":\"db1\"},{\"clusterList\":[{\"clusterName\":\"mha2\",\"nodes\":[{\"instancePort\":55111,\"instanceZoneId\":\"NTGXH\",\"role\":\"master\",\"ipBusiness\":\"11.11.11.3\"},{\"instancePort\":55111,\"instanceZoneId\":\"NTGXH\",\"role\":\"slave\",\"ipBusiness\":\"11.11.11.4\"}],\"env\":\"fat\",\"zoneId\":\"NTGXH\"},{\"clusterName\":\"sin1\",\"nodes\":[{\"instancePort\":55111,\"instanceZoneId\":\"sin-aws\",\"role\":\"master\",\"ipBusiness\":\"sin.rds.amazonaws.com\"}],\"env\":\"fat\",\"zoneId\":\"sin-aws\"}],\"dbName\":\"db2\"}]";
        List<DbClusterInfoDto> list = JsonUtils.fromJsonToList(json2, DbClusterInfoDto.class);
        when(dbaApiService.getDatabaseClusterInfoList("db1_dalcluster")).thenReturn(list);
        when(messengerServiceV2.checkMqConfig(any())).thenReturn(MqConfigCheckVo.from(Lists.newArrayList()));

        super.setUp();
    }

    private static MhaTblV2 getMhaTblV2(String mhaName) {
        MhaTblV2 srcMhaTbl = new MhaTblV2();
        srcMhaTbl.setMhaName(mhaName);
        return srcMhaTbl;
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
    public void testBuildAppliersEmpty() throws Exception {

        DrcBuildParam drcBuildParam = new DrcBuildParam();
        DrcBuildBaseParam srcBuildParam = new DrcBuildBaseParam();
        srcBuildParam.setMhaName("mha1");
        drcBuildParam.setSrcBuildParam(srcBuildParam);
        DrcBuildBaseParam dstBuildParam = new DrcBuildBaseParam();
        dstBuildParam.setApplierInitGtid("a:123");
        dstBuildParam.setMhaName("mha2");
        dstBuildParam.setDbApplierDtos(Lists.newArrayList(new DbApplierDto(Lists.newArrayList(), "gtidInit1", "db2")));
        drcBuildParam.setDstBuildParam(dstBuildParam);
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


    @Test
    public void testAutoConfigDbAppliers() throws Exception {
        when(resourceService.getMhaDbAvailableResource("mha2", ModuleEnum.APPLIER.getCode())).thenReturn(getResourceView());
        when(resourceService.handOffResource(anyList(), anyList())).thenReturn(getResourceView());
        dbDrcBuildService.autoConfigDbAppliers("mha1", "mha2", Lists.newArrayList("db1"), null, true);
    }


    @Test
    public void testGetExistDbReplicationDirections() {
//        List<DbDrcConfigInfoDto> testshard01db = dbDrcBuildService.getExistDbReplicationDirections("db1");
//        Assert.assertFalse(CollectionUtils.isEmpty(testshard01db));
    }

    @Test
    public void testGetDbDrcConfig() throws Exception {
//        DbDrcConfigInfoDto dbDrcConfig = dbDrcBuildService.getDbDrcConfig("db1", "src1", "dst1");
//        Assert.assertEquals("src1", dbDrcConfig.getSrcRegionName());
//        Assert.assertEquals("dst1", dbDrcConfig.getDstRegionName());
//        Assert.assertFalse(CollectionUtils.isEmpty(dbDrcConfig.getMhaReplications()));
//        System.out.println(dbDrcConfig);
    }


    @Test(expected = ConsoleException.class)
    public void createMhaDbDrcReplication() throws Exception {
        MhaDbReplicationCreateDto createDto = new MhaDbReplicationCreateDto();
        createDto.setSrcRegionName("src1");
        createDto.setDstRegionName("dst1");
        createDto.setDbName("db1");
        DrcAutoBuildParam drcAutoBuildParam = new DrcAutoBuildParam();
        drcAutoBuildParam.setSrcMhaName("mha1");
        drcAutoBuildParam.setDstMhaName("mha2");

        drcAutoBuildParam.setDbName(Sets.newHashSet("db1", "db2"));
        List<DrcAutoBuildParam> value = Lists.newArrayList(drcAutoBuildParam);
        when(drcAutoBuildService.getDrcBuildParam(any())).thenReturn(value);
        dbDrcBuildService.createMhaDbDrcReplication(createDto);
    }

    @Test()
    public void createMhaDbDrcMqReplication() throws Exception {
        MhaDbReplicationCreateDto createDto = new MhaDbReplicationCreateDto();
        createDto.setSrcRegionName("src1");
        createDto.setDbName("db1");
        createDto.setReplicationType(ReplicationTypeEnum.DB_TO_MQ.getType());

        DrcAutoBuildParam drcAutoBuildParam = new DrcAutoBuildParam();
        drcAutoBuildParam.setSrcMhaName("mha1");
        drcAutoBuildParam.setDbName(Sets.newHashSet("db3"));
        List<DrcAutoBuildParam> value = Lists.newArrayList(drcAutoBuildParam);
        when(drcAutoBuildService.getDrcBuildParam(any())).thenReturn(value);
        dbDrcBuildService.createMhaDbReplicationForMq(createDto);
        verify(drcBuildServiceV2, times(1)).buildMessengerMha(any());
        verify(drcBuildServiceV2, times(1)).syncMhaDbInfoFromDbaApiIfNeeded(any(), any());
        verify(drcBuildServiceV2, times(1)).autoConfigReplicatorsWithRealTimeGtid(any());
        verify(mhaDbReplicationService, times(1)).maintainMhaDbReplicationForMq(any(), any());
    }

    @Test
    public void createMhaDbDrcReplication2() throws Exception {
        MhaDbReplicationCreateDto createDto = new MhaDbReplicationCreateDto();
        createDto.setSrcRegionName("src1");
        createDto.setDstRegionName("dst1");
        createDto.setDbName("db5");
        DrcAutoBuildParam drcAutoBuildParam = new DrcAutoBuildParam();
        drcAutoBuildParam.setSrcMhaName("mha1");
        drcAutoBuildParam.setDstMhaName("mha2");

        drcAutoBuildParam.setDbName(Sets.newHashSet("db5"));
        List<DrcAutoBuildParam> value = Lists.newArrayList(drcAutoBuildParam);
        when(drcAutoBuildService.getDrcBuildParam(any())).thenReturn(value);
        dbDrcBuildService.createMhaDbDrcReplication(createDto);
    }

    @Test
    public void testEditDbReplication() throws Exception {
        DbReplicationEditDto editDto = getDbReplicationEditDto();
        dbDrcBuildService.editDbReplication(editDto);
        verify(drcBuildServiceV2, times(1)).buildDbReplicationConfig(any());

    }

    @Test
    public void testDeleteDbReplication() throws Exception {
        DbReplicationEditDto editDto = getDbReplicationEditDto();
        dbDrcBuildService.deleteDbReplication(editDto);
        verify(drcBuildServiceV2, times(1)).deleteDbReplications(editDto.getDbReplicationIds());
    }

    @Test
    public void testCreateDbReplication() throws Exception {
        DbReplicationCreateDto dbReplicationCreateDto = getDbReplicationCreateDto();
        dbDrcBuildService.createDbReplication(dbReplicationCreateDto);
        verify(drcBuildServiceV2, times(1)).buildDbReplicationConfig(any());

    }

    @Test
    public void testCreateDbMqReplication() throws Exception {
        DbMqCreateDto dbReplicationCreateDto = getDbMqCreateDto();
        dbDrcBuildService.createDbMqReplication(dbReplicationCreateDto);
        verify(messengerServiceV2, times(1)).processAddMqConfig(any());
    }

    @Test
    public void testEditDbMqReplication() throws Exception {
        DbMqEditDto editDto = getDbMqEditDto();
        dbDrcBuildService.editDbMqReplication(editDto);
        verify(messengerServiceV2, times(1)).processUpdateMqConfig(any());
    }

    @Test
    public void testDeleteDbMqReplication() throws Exception {
        DbMqEditDto editDto = getDbMqEditDto();
        dbDrcBuildService.deleteDbMqReplication(editDto);
        verify(messengerServiceV2, times(1)).processDeleteMqConfig(any());
    }


    @Test(expected = ConsoleException.class)
    public void testDeleteDbMqReplicationInvalidReq() throws Exception {
        DbMqEditDto editDto = getDbMqEditDto();
        editDto.getOriginLogicTableConfig().setMessengerFilterId(null);
        dbDrcBuildService.deleteDbMqReplication(editDto);
        verify(messengerServiceV2, times(1)).processDeleteMqConfig(any());
    }

    @Test
    public void testSwitchAppliers() throws Exception {
        List<DbApplierSwitchReqDto> reqDtos = new ArrayList<>();
        DbApplierSwitchReqDto req1 = new DbApplierSwitchReqDto();
        req1.setDbNames(Lists.newArrayList("db1"));
        req1.setSrcMhaName("mha1");
        req1.setDstMhaName("mha2");
        reqDtos.add(req1);

        DbApplierSwitchReqDto req2 = new DbApplierSwitchReqDto();
        req2.setDbNames(Lists.newArrayList("aadb1"));
        req2.setSrcMhaName("mha2");
        req2.setDstMhaName("mha1");
        reqDtos.add(req2);

        when(defaultConsoleConfig.getNewDrcDefaultDbApplierMode()).thenReturn(false);
        dbDrcBuildService.switchAppliers(reqDtos);
        verify(drcBuildServiceV2, times(1)).autoConfigAppliers(any(), any(), any(), Mockito.anyBoolean());
        verify(applierTblV3Dao, times(1)).batchInsert(any());

        when(defaultConsoleConfig.getNewDrcDefaultDbApplierMode()).thenReturn(true);
        when(mysqlServiceV2.getMhaExecutedGtid(anyString())).thenReturn("abc:123");
        dbDrcBuildService.switchAppliers(reqDtos);
        verify(drcBuildServiceV2, times(2)).getMhaAppliers(any(), any());
        verify(applierTblV3Dao, times(2)).batchInsert(any());
    }

    @Test
    public void testSwitchMessengers() throws Exception {
        List<DbApplierSwitchReqDto> reqDtos = new ArrayList<>();
        DbApplierSwitchReqDto req2 = new DbApplierSwitchReqDto();
        req2.setDbNames(Lists.newArrayList("aadb1"));
        req2.setSrcMhaName("mha2");
        reqDtos.add(req2);

        dbDrcBuildService.switchMessengers(reqDtos);
        verify(drcBuildServiceV2, times(1)).autoConfigMessengersWithRealTimeGtid(any(MhaTblV2.class),anyBoolean());
    }

    @Test
    public void testSwitchMessengersM() throws Exception {
        List<DbApplierSwitchReqDto> reqDtos = new ArrayList<>();
        DbApplierSwitchReqDto req2 = new DbApplierSwitchReqDto();
        req2.setDbNames(Lists.newArrayList("aadb1"));
        req2.setSrcMhaName("mha2");
        reqDtos.add(req2);

        dbDrcBuildService.switchMessengersM(reqDtos);
        verify(drcBuildServiceV2, times(1)).autoConfigMessengersWithRealTimeGtidM(any(MhaTblV2.class),anyBoolean());
    }


    @Test(expected = IllegalArgumentException.class)
    public void testSwitchMessengersDbMessengerNotSupport() throws Exception {
        List<DbApplierSwitchReqDto> reqDtos = new ArrayList<>();
        DbApplierSwitchReqDto req1 = new DbApplierSwitchReqDto();
        req1.setDbNames(Lists.newArrayList("db1"));
        req1.setSrcMhaName("mha1");
        reqDtos.add(req1);


        dbDrcBuildService.switchMessengers(reqDtos);
    }

    private static DbReplicationEditDto getDbReplicationEditDto() {
        DbReplicationEditDto editDto = new DbReplicationEditDto();
        LogicTableConfig originLogicTableConfig = new LogicTableConfig();
        originLogicTableConfig.setLogicTable("old_table");
        editDto.setOriginLogicTableConfig(originLogicTableConfig);
        LogicTableConfig logicTableConfig = new LogicTableConfig();
        logicTableConfig.setLogicTable("new table");
        editDto.setLogicTableConfig(logicTableConfig);
        editDto.setDbNames(Lists.newArrayList("db1", "db2"));
        editDto.setSrcRegionName("src1");
        editDto.setDstRegionName("dst1");
        editDto.setColumnsFilterCreateParam(new ColumnsFilterCreateParam());
        editDto.setRowsFilterCreateParam(new RowsFilterCreateParam());

        editDto.setDbReplicationIds(Lists.newArrayList(1001L, 1002L));
        return editDto;
    }


    private static DbMqCreateDto getDbMqCreateDto() {
        DbMqCreateDto create = new DbMqCreateDto();
        LogicTableConfig logicTableConfig = new LogicTableConfig();
        logicTableConfig.setLogicTable("new table");
        logicTableConfig.setDstLogicTable("bbz.test.binlog");
        create.setLogicTableConfig(logicTableConfig);
        create.setDbNames(Lists.newArrayList("db1", "db2"));
        create.setSrcRegionName("src1");
        MqConfigDto mqConfig = new MqConfigDto();
        mqConfig.setOrder(true);
        mqConfig.setOrderKey("orderId");
        mqConfig.setBu("bbz");
        mqConfig.setMqType("qmq");
        mqConfig.setSerialization("json");
        create.setMqConfig(mqConfig);

        return create;
    }

    private static DbMqEditDto getDbMqEditDto() {
        DbMqEditDto editDto = new DbMqEditDto();
        LogicTableConfig logicTableConfig = new LogicTableConfig();
        logicTableConfig.setLogicTable("new table");
        logicTableConfig.setDstLogicTable("bbz.test.binlog");
        editDto.setLogicTableConfig(logicTableConfig);
        LogicTableConfig originLogicTableConfig = new LogicTableConfig();
        originLogicTableConfig.setLogicTable("old_table");
        originLogicTableConfig.setDstLogicTable("bbz.test.binlog");
        originLogicTableConfig.setMessengerFilterId(1L);
        editDto.setOriginLogicTableConfig(originLogicTableConfig);
        editDto.setDbNames(Lists.newArrayList("db1", "db2"));
        editDto.setSrcRegionName("src1");
        MqConfigDto mqConfig = new MqConfigDto();
        mqConfig.setOrder(true);
        mqConfig.setOrderKey("orderId");
        mqConfig.setBu("bbz");
        mqConfig.setMqType("qmq");
        mqConfig.setSerialization("json");
        editDto.setMqConfig(mqConfig);

        editDto.setDbReplicationIds(Lists.newArrayList(1005L, 1006L));
        return editDto;
    }

    private static DbReplicationCreateDto getDbReplicationCreateDto() {
        DbReplicationCreateDto create = new DbReplicationCreateDto();
        LogicTableConfig logicTableConfig = new LogicTableConfig();
        logicTableConfig.setLogicTable("new table");
        create.setLogicTableConfig(logicTableConfig);
        create.setDbNames(Lists.newArrayList("db1", "db2"));
        create.setSrcRegionName("src1");
        create.setDstRegionName("dst1");
        create.setColumnsFilterCreateParam(new ColumnsFilterCreateParam());
        create.setRowsFilterCreateParam(new RowsFilterCreateParam());

        return create;
    }

    private List<ResourceView> getResourceView() {
        ResourceView v1 = new ResourceView();
        v1.setIp("2.113.60.1");
        ResourceView v2 = new ResourceView();
        v2.setIp("2.113.60.2");
        return Lists.newArrayList(v1, v2);
    }
}
