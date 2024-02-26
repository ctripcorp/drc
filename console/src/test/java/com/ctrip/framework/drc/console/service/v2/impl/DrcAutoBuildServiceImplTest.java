package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.ReplicatorGroupTblDao;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.ApplierGroupTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.MhaReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.dto.v2.MachineDto;
import com.ctrip.framework.drc.console.param.v2.ColumnsFilterCreateParam;
import com.ctrip.framework.drc.console.param.v2.DrcAutoBuildParam;
import com.ctrip.framework.drc.console.param.v2.DrcAutoBuildReq;
import com.ctrip.framework.drc.console.param.v2.DrcAutoBuildReq.TblsFilterDetail;
import com.ctrip.framework.drc.console.param.v2.RowsFilterCreateParam;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.DbDrcBuildService;
import com.ctrip.framework.drc.console.service.v2.DrcBuildServiceV2;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.ClusterInfoDto;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.DbClusterInfoDto;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.check.TableCheckVo;
import com.ctrip.framework.drc.console.vo.display.v2.MhaReplicationPreviewDto;
import com.ctrip.framework.drc.core.driver.binlog.impl.TransactionContext;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.springframework.util.CollectionUtils;

import java.util.*;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

public class DrcAutoBuildServiceImplTest {
    @Mock
    Logger logger;
    @Mock
    MetaInfoServiceV2 metaInfoService;
    @Mock
    MhaTblV2Dao mhaTblDao;
    @Mock
    MhaReplicationTblDao mhaReplicationTblDao;
    @Mock
    ReplicatorGroupTblDao replicatorGroupTblDao;
    @Mock
    ApplierGroupTblV2Dao applierGroupTblDao;
    @Mock
    MysqlServiceV2 mysqlServiceV2;
    @Mock
    DbaApiService dbaApiService;
    @Mock
    DefaultConsoleConfig consoleConfig;
    @Mock
    DrcBuildServiceV2 drcBuildService;
    @InjectMocks
    DrcAutoBuildServiceImpl drcAutoBuildServiceImpl;
    @Mock
    DbDrcBuildService dbDrcBuildService;

    public static final String TEST_DB_NAME = "testDb";
    public static final String TEST_DB_NAME2 = "testDb_2";
    public static final String DAL_CLUSTER_NAME = "test_dalcluster";

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        String json1 = "[{\"clusterName\":\"mha1\",\"nodes\":[{\"instancePort\":55111,\"instanceZoneId\":\"NTGXH\",\"role\":\"master\",\"ipBusiness\":\"10.11.11.10\"},{\"instancePort\":55111,\"instanceZoneId\":\"NTGXH\",\"role\":\"slave\",\"ipBusiness\":\"10.11.11.11\"}],\"env\":\"fat\",\"zoneId\":\"NTGXH\"},{\"clusterName\":\"sin1\",\"nodes\":[{\"instancePort\":55111,\"instanceZoneId\":\"sin-aws\",\"role\":\"master\",\"ipBusiness\":\"sinrds.com\"}],\"env\":\"fat\",\"zoneId\":\"sin-aws\"}]";
        List<ClusterInfoDto> dbClusterInfoDtos1 = JsonUtils.fromJsonToList(json1, ClusterInfoDto.class);
        when(dbaApiService.getDatabaseClusterInfo(TEST_DB_NAME)).thenReturn(dbClusterInfoDtos1);
        when(dbaApiService.getDatabaseClusterInfo(TEST_DB_NAME2)).thenReturn(dbClusterInfoDtos1);

        String json2 = "[{\"clusterList\":[{\"clusterName\":\"mha1\",\"nodes\":[{\"instancePort\":55111,\"instanceZoneId\":\"NTGXH\",\"role\":\"master\",\"ipBusiness\":\"11.11.11.1\"},{\"instancePort\":55111,\"instanceZoneId\":\"NTGXH\",\"role\":\"slave\",\"ipBusiness\":\"11.11.11.2\"}],\"env\":\"fat\",\"zoneId\":\"NTGXH\"},{\"clusterName\":\"sin1\",\"nodes\":[{\"instancePort\":55111,\"instanceZoneId\":\"sin-aws\",\"role\":\"master\",\"ipBusiness\":\"sin.rds.amazonaws.com\"}],\"env\":\"fat\",\"zoneId\":\"sin-aws\"}],\"dbName\":\"testshard01db\"},{\"clusterList\":[{\"clusterName\":\"mha2\",\"nodes\":[{\"instancePort\":55111,\"instanceZoneId\":\"NTGXH\",\"role\":\"master\",\"ipBusiness\":\"11.11.11.3\"},{\"instancePort\":55111,\"instanceZoneId\":\"NTGXH\",\"role\":\"slave\",\"ipBusiness\":\"11.11.11.4\"}],\"env\":\"fat\",\"zoneId\":\"NTGXH\"},{\"clusterName\":\"sin1\",\"nodes\":[{\"instancePort\":55111,\"instanceZoneId\":\"sin-aws\",\"role\":\"master\",\"ipBusiness\":\"sin.rds.amazonaws.com\"}],\"env\":\"fat\",\"zoneId\":\"sin-aws\"}],\"dbName\":\"testshard02db\"}]";
        List<DbClusterInfoDto> list = JsonUtils.fromJsonToList(json2, DbClusterInfoDto.class);
        when(dbaApiService.getDatabaseClusterInfoList("test_dalcluster")).thenReturn(list);

        Map<String, String> map = new HashMap<>();
        String dbaDc2DrcDcMap = "{\"ntgxh\":\"ntgxh\",\"ntgxy\":\"ntgxy\",\"fra-aws\":\"fraaws\",\"sin-aws\":\"sinaws\",\"sha-ali\":\"shali\",\"sharb\":\"sharb\",\"shaxy\":\"shaxy\",\"shafq\":\"shaxy\",\"shalk\":\"shaxy\",\"shajz\":\"shaxy\"}";
        Map map1 = JsonUtils.fromJson(dbaDc2DrcDcMap, Map.class);
        when(consoleConfig.getDbaDc2DrcDcMap()).thenReturn(map1);

        String dcDoList = "[{\"dcId\":26,\"dcName\":\"shaoy\",\"regionId\":1,\"regionName\":\"sha\"},{\"dcId\":27,\"dcName\":\"sharb\",\"regionId\":1,\"regionName\":\"sha\"},{\"dcId\":30,\"dcName\":\"ntgxh\",\"regionId\":3,\"regionName\":\"ntgxh\"},{\"dcId\":32,\"dcName\":\"ntgxy\",\"regionId\":4,\"regionName\":\"ntgxy\"},{\"dcId\":34,\"dcName\":\"shali\",\"regionId\":1,\"regionName\":\"sha\"},{\"dcId\":35,\"dcName\":\"shaxy\",\"regionId\":1,\"regionName\":\"sha\"},{\"dcId\":36,\"dcName\":\"sinaws\",\"regionId\":2,\"regionName\":\"sin\"},{\"dcId\":37,\"dcName\":\"fraaws\",\"regionId\":5,\"regionName\":\"fra\"}]";
        when(metaInfoService.queryAllDcWithCache()).thenReturn(JsonUtils.fromJsonToList(dcDoList, DcDo.class));


        when(mysqlServiceV2.preCheckMySqlTables("mha2", "(testshard02db)\\.testTable1")).thenReturn(Collections.singletonList(new TableCheckVo(new MySqlUtils.TableSchemaName("testshard01db", "testTable1"))));
        when(mysqlServiceV2.preCheckMySqlTables("mha1", "(testshard01db)\\.testTable1")).thenReturn(Collections.singletonList(new TableCheckVo(new MySqlUtils.TableSchemaName("testshard02db", "testTable1"))));

    }


    @Test
    public void testGetDbClusterInfoDtos() {
        // test db
        DrcAutoBuildReq buildReq = getDrcAutoBuildReqForSingleDb();


        List<DbClusterInfoDto> dbClusterInfoDtos = drcAutoBuildServiceImpl.getDbClusterInfoDtos(buildReq);
        Assert.assertEquals(1, dbClusterInfoDtos.size());
        Assert.assertEquals(TEST_DB_NAME, dbClusterInfoDtos.get(0).getDbName());
        Assert.assertTrue(dbClusterInfoDtos.get(0).getClusterList().size() > 0);


        // test dal cluster
        DrcAutoBuildReq buildReq2 = getDrcAutoBuildReqForDalCluster();

        List<DbClusterInfoDto> dbClusterInfoDtos2 = drcAutoBuildServiceImpl.getDbClusterInfoDtos(buildReq2);
        Assert.assertEquals(2, dbClusterInfoDtos2.size());
        Assert.assertEquals("testshard01db", dbClusterInfoDtos2.get(0).getDbName());
        Assert.assertEquals("testshard02db", dbClusterInfoDtos2.get(1).getDbName());
        Assert.assertTrue(dbClusterInfoDtos2.get(0).getClusterList().size() > 0);
    }


    @Test
    public void testGetRegionNameOptions() {
        DrcAutoBuildReq drcAutoBuildReqForSingleDb = getDrcAutoBuildReqForSingleDb();
        List<String> regionOptions = drcAutoBuildServiceImpl.getRegionOptions(drcAutoBuildReqForSingleDb);
        Assert.assertEquals(2, regionOptions.size());
        Assert.assertTrue(regionOptions.contains("sin"));
        Assert.assertTrue(regionOptions.contains("ntgxh"));
        Assert.assertFalse(regionOptions.contains("ntxrb"));

    }

    @Test
    public void testPreCheckMhaReplication() throws Exception {
        DrcAutoBuildReq req = getDrcAutoBuildReqForDalCluster();
        req.setSrcRegionName("ntgxh");
        req.setDstRegionName("sin");

        List<MhaReplicationPreviewDto> result = drcAutoBuildServiceImpl.preCheckMhaReplication(req);
        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.stream().allMatch(e -> e.getSrcRegionName().equals("ntgxh")));
        Assert.assertTrue(result.stream().allMatch(e -> e.getDstRegionName().equals("sin")));
        Assert.assertEquals("testshard01db", result.get(0).getDbName());
        Assert.assertEquals("mha1", result.get(0).getSrcMha().getName());
    }


    @Test
    public void testGetDrcBuildParam() {
        DrcAutoBuildReq req = getDrcAutoBuildReqForDalCluster();

        List<DrcAutoBuildParam> drcBuildParam = drcAutoBuildServiceImpl.getDrcBuildParam(req);
        Assert.assertEquals(2, drcBuildParam.size());
        Assert.assertFalse(StringUtils.isBlank(drcBuildParam.get(0).getSrcMhaName()));
        List<MachineDto> srcMachines = drcBuildParam.get(0).getSrcMachines();
        Assert.assertFalse(CollectionUtils.isEmpty(srcMachines));
        MachineDto machineDto = srcMachines.get(0);
        Assert.assertTrue(machineDto.getIp().contains("11.11.11"));
        Assert.assertEquals(55111, (int) machineDto.getPort());
        Assert.assertFalse(CollectionUtils.isEmpty(drcBuildParam.get(0).getDstMachines()));


        req = getDrcAutoBuildReqForSingleDb();
        drcBuildParam = drcAutoBuildServiceImpl.getDrcBuildParam(req);
        Assert.assertEquals(1, drcBuildParam.size());
        Assert.assertFalse(StringUtils.isBlank(drcBuildParam.get(0).getSrcMhaName()));



        req = getDrcAutoBuildReqForMultiDb();
        drcBuildParam = drcAutoBuildServiceImpl.getDrcBuildParam(req);
        Assert.assertEquals(1, drcBuildParam.size());
        Assert.assertEquals(2, drcBuildParam.get(0).getDbName().size());
        Assert.assertFalse(StringUtils.isBlank(drcBuildParam.get(0).getSrcMhaName()));
    }


    @Test(expected = IllegalArgumentException.class)
    public void testGetDrcBuildParamNoMode() {
        DrcAutoBuildReq req = getDrcAutoBuildReqForDalCluster();
        req.setMode(null);
        drcAutoBuildServiceImpl.getDrcBuildParam(req);
    }

    @Test(expected = Exception.class)
    public void testGetDrcBuildParamNullRegion() {
        DrcAutoBuildReq req = getDrcAutoBuildReqForDalCluster();
        req.setSrcRegionName(null);
        drcAutoBuildServiceImpl.getDrcBuildParam(req);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetDrcBuildParamNoRegion() {
        DrcAutoBuildReq req = getDrcAutoBuildReqForDalCluster();
        req.setSrcRegionName("noSuchRegion");
        drcAutoBuildServiceImpl.getDrcBuildParam(req);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetDrcBuildParamNoRegion2() {
        DrcAutoBuildReq req = getDrcAutoBuildReqForDalCluster();
        req.setDstRegionName("noSuchRegion");
        drcAutoBuildServiceImpl.getDrcBuildParam(req);
    }

    @Test(expected = Exception.class)
    public void testGetDrcBuildParamSameRegion() {
        DrcAutoBuildReq req = getDrcAutoBuildReqForDalCluster();
        req.setSrcRegionName("sin");
        req.setDstRegionName("sin");
        drcAutoBuildServiceImpl.getDrcBuildParam(req);
    }


    @Test(expected = IllegalArgumentException.class)
    public void testGetDrcBuildParamNoTbl() {
        DrcAutoBuildReq req = getDrcAutoBuildReqForDalCluster();
        req.setTblsFilterDetail(null);

        drcAutoBuildServiceImpl.getDrcBuildParam(req);
    }

    @Test(expected = Exception.class)
    public void testGetDrcBuildParamNoDB() {
        DrcAutoBuildReq req = getDrcAutoBuildReqForSingleDb();
        req.setDbName(null);
        drcAutoBuildServiceImpl.getDrcBuildParam(req);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetDrcBuildParamNoDalCluster() {
        DrcAutoBuildReq req = getDrcAutoBuildReqForDalCluster();
        req.setDalClusterName(null);
        drcAutoBuildServiceImpl.getDrcBuildParam(req);
    }


    @Test
    public void testPreCheckMysqlTables() {
        DrcAutoBuildReq req = getDrcAutoBuildReqForDalCluster();
        List<TableCheckVo> tableCheckVos = drcAutoBuildServiceImpl.preCheckMysqlTables(req);
        Assert.assertEquals(2, tableCheckVos.size());


        req.getTblsFilterDetail().setTableNames("noSuchTable");
        tableCheckVos = drcAutoBuildServiceImpl.preCheckMysqlTables(req);
        Assert.assertEquals(0, tableCheckVos.size());
    }

    @Test
    public void testGetCommonColumn() {
        DrcAutoBuildReq req = getDrcAutoBuildReqForDalCluster();

        when(mysqlServiceV2.getCommonColumnIn("mha1", "(testshard01db)", "testTable1")).thenReturn(Sets.newHashSet("column1", "column2"));
        when(mysqlServiceV2.getCommonColumnIn("mha2", "(testshard02db)", "testTable1")).thenReturn(Sets.newHashSet("column1", "column3"));


        List<String> commonColumn = drcAutoBuildServiceImpl.getCommonColumn(req);
        Assert.assertEquals(Lists.newArrayList("column1"), commonColumn);
    }

    @Test
    public void testGetCommonColumn2() {
        DrcAutoBuildReq req = getDrcAutoBuildReqForDalCluster();

        when(mysqlServiceV2.getCommonColumnIn("mha1", "(testshard01db)", "testTable1")).thenReturn(Sets.newHashSet("column1", "column3"));
        when(mysqlServiceV2.getCommonColumnIn("mha2", "(testshard02db)", "testTable1")).thenReturn(Sets.newHashSet());

        List<String> commonColumn = drcAutoBuildServiceImpl.getCommonColumn(req);
        Assert.assertEquals(Lists.newArrayList(), commonColumn);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAutoBuildNoBu() throws Exception {
        DrcAutoBuildReq req = getDrcAutoBuildReqForSingleDb();
        req.setBuName("BBZ");
        req.setTag(null);
        drcAutoBuildServiceImpl.autoBuildDrc(req);
    }
    @Test(expected = IllegalArgumentException.class)
    public void testAutoBuildNoTag() throws Exception {
        DrcAutoBuildReq req = getDrcAutoBuildReqForSingleDb();
        req.setBuName(null);
        req.setTag("COMMON");
        drcAutoBuildServiceImpl.autoBuildDrc(req);
    }
    @Test
    public void testAutoBuild() throws Exception {
        DrcAutoBuildReq req = getDrcAutoBuildReqForSingleDb();
        req.setBuName("BBZ");
        req.setTag("COMMON");

        req.setOpenRowsFilterConfig(true);
        req.setRowsFilterDetail(new RowsFilterCreateParam());
        req.setOpenColsFilterConfig(true);
        req.setColsFilterDetail(new ColumnsFilterCreateParam());

        // mock mha
        String mhaJson = "[{\"id\":1,\"mhaName\":\"mha1\",\"clusterName\":\"test_dalcluster\",\"dcId\":1,\"buId\":1,\"monitorSwitch\":0,\"applyMode\":0,\"deleted\":0},{\"id\":2,\"mhaName\":\"mha2\",\"clusterName\":\"test_dalcluster\",\"dcId\":2,\"buId\":1,\"monitorSwitch\":0,\"applyMode\":0,\"deleted\":0},{\"id\":3,\"mhaName\":\"sin1\",\"clusterName\":\"test_dalcluster\",\"dcId\":3,\"buId\":1,\"monitorSwitch\":0,\"applyMode\":0,\"deleted\":0}]";
        List<MhaTblV2> mhaTblV2List = JsonUtils.fromJsonToList(mhaJson, MhaTblV2.class);
        when(mhaTblDao.queryByMhaName(anyString(), anyInt())).thenAnswer(i -> {
            String mhaName = i.getArgument(0, String.class);
            Integer deleted = i.getArgument(1, Integer.class);
            return mhaTblV2List.stream().filter(e -> mhaName.equals(e.getMhaName()) && deleted.equals(e.getDeleted())).findFirst().orElse(null);
        });

        String mhaReplicationJson = "[{\"id\":1,\"srcMhaId\":1,\"dstMhaId\":3,\"deleted\":0,\"drcStatus\":1,\"createTime\":\"2025-07-13 03:46:15\",\"datachangeLasttime\":\"2015-09-15 04:37:01\"},{\"id\":2,\"srcMhaId\":2,\"dstMhaId\":3,\"deleted\":0,\"drcStatus\":1,\"createTime\":\"2025-07-13 03:46:15\",\"datachangeLasttime\":\"2015-09-15 04:37:01\"},{\"id\":3,\"srcMhaId\":3,\"dstMhaId\":1,\"deleted\":0,\"drcStatus\":1,\"createTime\":\"2025-07-13 03:46:15\",\"datachangeLasttime\":\"2015-09-15 04:37:01\"},{\"id\":4,\"srcMhaId\":3,\"dstMhaId\":2,\"deleted\":0,\"drcStatus\":1,\"createTime\":\"2025-07-13 03:46:15\",\"datachangeLasttime\":\"2015-09-15 04:37:01\"}]";
        List<MhaReplicationTbl> mhaReplicationTbls = JsonUtils.fromJsonToList(mhaReplicationJson, MhaReplicationTbl.class);
        when(mhaReplicationTblDao.queryByMhaId(anyLong(), anyLong(), anyInt())).thenAnswer(i -> {
            Long srcMhaId = i.getArgument(0, Long.class);
            Long dstMhaId = i.getArgument(1, Long.class);
            Integer deleted = i.getArgument(2, Integer.class);
            return mhaReplicationTbls.stream().filter(e -> e.getSrcMhaId().equals(srcMhaId) && e.getDstMhaId().equals(dstMhaId) && Objects.equals(e.getDeleted(), deleted)).findFirst().orElse(null);
        });
        when(mysqlServiceV2.getMhaExecutedGtid(any())).thenReturn("26ddaa00-4d3f-11ee-bd11-06cc389a9314:1-11286566");

        drcAutoBuildServiceImpl.autoBuildDrc(req);
        verify(drcBuildService, times(1)).buildMha(any());
        verify(drcBuildService, times(2)).syncMhaDbInfoFromDbaApiIfNeeded(any(), any());
        verify(drcBuildService, times(1)).buildDbReplicationConfig(any());
        verify(drcBuildService, times(1)).autoConfigReplicatorsWithRealTimeGtid(any());
        verify(drcBuildService, times(1)).autoConfigReplicatorsWithGtid(any(),any());
        verify(drcBuildService, times(1)).autoConfigAppliers(any(), any(), any());
    }


    private static DrcAutoBuildReq getDrcAutoBuildReqForDalCluster() {
        DrcAutoBuildReq req = new DrcAutoBuildReq();
        req.setDalClusterName(DAL_CLUSTER_NAME);
        req.setMode(DrcAutoBuildReq.BuildMode.DAL_CLUSTER_NAME.getValue());
        req.setSrcRegionName("ntgxh");
        req.setDstRegionName("sin");
        TblsFilterDetail tblsFilterDetail = new TblsFilterDetail();
        tblsFilterDetail.setTableNames("testTable1");
        req.setTblsFilterDetail(tblsFilterDetail);

        return req;
    }

    private static DrcAutoBuildReq getDrcAutoBuildReqForSingleDb() {
        DrcAutoBuildReq req = new DrcAutoBuildReq();
        req.setMode(DrcAutoBuildReq.BuildMode.SINGLE_DB_NAME.getValue());
        req.setDbName(TEST_DB_NAME);
        req.setSrcRegionName("ntgxh");
        req.setDstRegionName("sin");
        TblsFilterDetail tblsFilterDetail = new TblsFilterDetail();
        tblsFilterDetail.setTableNames("testTable1");
        req.setTblsFilterDetail(tblsFilterDetail);

        return req;
    }


    private static DrcAutoBuildReq getDrcAutoBuildReqForMultiDb() {
        DrcAutoBuildReq req = new DrcAutoBuildReq();
        req.setMode(DrcAutoBuildReq.BuildMode.MULTI_DB_NAME.getValue());
        req.setDbName(String.join(",",TEST_DB_NAME,TEST_DB_NAME2));
        req.setSrcRegionName("ntgxh");
        req.setDstRegionName("sin");
        TblsFilterDetail tblsFilterDetail = new TblsFilterDetail();
        tblsFilterDetail.setTableNames("testTable1");
        req.setTblsFilterDetail(tblsFilterDetail);

        return req;
    }
}