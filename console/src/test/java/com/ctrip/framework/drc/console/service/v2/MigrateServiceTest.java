package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.ApplierGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.param.NameFilterSplitParam;
import com.ctrip.framework.drc.console.service.impl.DalServiceImpl;
import com.ctrip.framework.drc.console.service.v2.impl.MetaMigrateServiceImpl;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.api.MhaNameFilterVo;
import com.ctrip.framework.drc.console.vo.check.TableCheckVo;
import com.ctrip.framework.drc.console.vo.response.migrate.MhaDbMappingResult;
import com.ctrip.framework.drc.console.vo.response.migrate.MigrateMhaDbMappingResult;
import com.ctrip.framework.drc.console.vo.response.migrate.MigrateResult;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.foundation.Env;
import com.ctrip.platform.dal.dao.DalHints;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.monitor.MockTest.times;
import static com.ctrip.framework.drc.console.service.v2.MetaGeneratorBuilder.getDbTbls;

/**
 * Created by dengquanliang
 * 2023/6/15 11:21
 */
public class MigrateServiceTest {
    @InjectMocks
    private MetaMigrateServiceImpl migrationService;
    @Mock
    private MhaTblV2Dao mhaTblV2Dao;
    @Mock
    private MhaTblDao mhaTblDao;
    @Mock
    private MhaGroupTblDao mhaGroupTblDao;
    @Mock
    private GroupMappingTblDao groupMappingTblDao;
    @Mock
    private ClusterTblDao clusterTblDao;
    @Mock
    private ClusterMhaMapTblDao clusterMhaMapTblDao;
    @Mock
    private MhaReplicationTblDao mhaReplicationTblDao;
    @Mock
    private ApplierGroupTblV2Dao applierGroupTblV2Dao;
    @Mock
    private ApplierGroupTblDao applierGroupTblDao;
    @Mock
    private ApplierTblDao applierTblDao;
    @Mock
    private ApplierTblV2Dao applierTblV2Dao;
    @Mock
    private DbReplicationTblDao dbReplicationTblDao;
    @Mock
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Mock
    private DbTblDao dbTblDao;
    @Mock
    private BuTblDao buTblDao;
    @Mock
    private RouteTblDao routeTblDao;
    @Mock
    private ProxyTblDao proxyTblDao;
    @Mock
    private DcTblDao dcTblDao;
    @Mock
    private ResourceTblDao resourceTblDao;
    @Mock
    private MachineTblDao machineTblDao;
    @Mock
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Mock
    private ClusterManagerTblDao clusterManagerTblDao;
    @Mock
    private ZookeeperTblDao zookeeperTblDao;
    @Mock
    private ReplicatorTblDao replicatorTblDao;
    @Mock
    private DataMediaTblDao dataMediaTblDao;
    @Mock
    private DataMediaPairTblDao dataMediaPairTblDao;
    @Mock
    private MessengerGroupTblDao messengerGroupTblDao;
    @Mock
    private MessengerFilterTblDao messengerFilterTblDao;
    @Mock
    private RowsFilterMappingTblDao rowsFilterMappingTblDao;
    @Mock
    private RowsFilterTblDao rowsFilterTblDao;
    @Mock
    private RegionTblDao regionTblDao;
    @Mock
    private ColumnsFilterTblDao columnsFilterTblDao;
    @Mock
    private ColumnsFilterTblV2Dao columnFilterTblV2Dao;
    @Mock
    private DbReplicationFilterMappingTblDao dbReplicationFilterMappingTblDao;
    @Mock
    private DalServiceImpl dalService;
    @Mock
    private DbClusterSourceProvider dbClusterSourceProvider;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        Mockito.when(mhaTblDao.queryAll()).thenReturn(MigrateEntityBuilder.getMhaTbls());
        Mockito.when(mhaTblV2Dao.queryAll()).thenReturn(MetaGeneratorBuilder.getMhaTbls());
        Mockito.when(mhaGroupTblDao.queryAll()).thenReturn(MigrateEntityBuilder.getMhaGroups());
        Mockito.when(groupMappingTblDao.queryAll()).thenReturn(MigrateEntityBuilder.getGroupMappings());

        Mockito.when(replicatorGroupTblDao.queryAll()).thenReturn(MigrateEntityBuilder.getReplicatorGroupTbls());
        Mockito.when(applierGroupTblDao.queryAll()).thenReturn(MigrateEntityBuilder.getApplierGroupTbls());
        Mockito.when(applierTblDao.queryAll()).thenReturn(MigrateEntityBuilder.getApplierTbls());
        Mockito.when(mhaReplicationTblDao.queryAll()).thenReturn(MigrateEntityBuilder.getMhaReplicationTbls());
    }

    @Test
    public void testBatchInsertRegions() throws Exception {
        Mockito.when(regionTblDao.batchInsert(Mockito.anyList())).thenReturn(new int[0]);
        int result = migrationService.batchInsertRegions(Lists.newArrayList("region"));
        Assert.assertEquals(result, 1);
    }

    @Test
    public void testMigrateMhaTbl() throws Exception {
        Mockito.when(clusterTblDao.queryAll()).thenReturn(MigrateEntityBuilder.getClusterTbls());
        Mockito.when(clusterMhaMapTblDao.queryAll()).thenReturn(MigrateEntityBuilder.getClusterMhaMapTbl());
        Mockito.when(mhaTblV2Dao.batchInsert(Mockito.any(DalHints.class), Mockito.anyList())).thenReturn(new int[0]);
        Mockito.when(mhaTblV2Dao.batchUpdate(Mockito.anyList())).thenReturn(new int[0]);

        MigrateResult result = migrationService.migrateMhaTbl();
        Mockito.verify(mhaTblV2Dao, times(1)).batchInsert(Mockito.any(DalHints.class), Mockito.anyList());
        Mockito.verify(mhaTblV2Dao, times(2)).batchUpdate(Mockito.anyList());
        Assert.assertEquals(result.getInsertSize(), 9);
        Assert.assertEquals(result.getUpdateSize(), 1);
    }

    @Test
    public void testMigrateMhaReplication() throws Exception {
        Mockito.when(mhaReplicationTblDao.batchInsert(Mockito.anyList())).thenReturn(new int[0]);
        MigrateResult result = migrationService.migrateMhaReplication();
        Mockito.verify(mhaReplicationTblDao, times(1)).batchInsert(Mockito.anyList());
        Mockito.verify(mhaReplicationTblDao, times(2)).batchUpdate(Mockito.anyList());
        Assert.assertEquals(result.getInsertSize(), 9);
        Assert.assertEquals(result.getUpdateSize(), 1);
    }

    @Test
    public void testMigrateApplier() throws Exception {
        Mockito.when(applierTblV2Dao.queryAll()).thenReturn(MetaGeneratorBuilder.getApplierTbls());
        Mockito.when(applierTblV2Dao.batchUpdate(Mockito.anyList())).thenReturn(new int[0]);
        Mockito.when(applierTblV2Dao.batchInsert(Mockito.any(DalHints.class), Mockito.anyList())).thenReturn(new int[0]);

        MigrateResult result = migrationService.migrateApplier();
        Mockito.verify(applierTblV2Dao, times(1)).batchInsert(Mockito.any(DalHints.class), Mockito.anyList());
        Mockito.verify(applierTblV2Dao, times(2)).batchUpdate(Mockito.anyList());
        Assert.assertEquals(result.getInsertSize(), 9);
        Assert.assertEquals(result.getUpdateSize(), 1);
    }

    @Test
    public void testMigrateApplierGroup() throws Exception {
        Mockito.when(applierGroupTblV2Dao.queryAll()).thenReturn(MetaGeneratorBuilder.getApplierGroupTbls());
        Mockito.when(applierGroupTblV2Dao.batchUpdate(Mockito.anyList())).thenReturn(new int[0]);
        Mockito.when(applierGroupTblV2Dao.batchInsert(Mockito.any(DalHints.class), Mockito.anyList())).thenReturn(new int[0]);
        Mockito.when(mhaReplicationTblDao.batchInsert(Mockito.anyList())).thenReturn(new int[0]);

        MigrateResult result = migrationService.migrateApplierGroup();
        Mockito.verify(applierGroupTblV2Dao, times(1)).batchInsert(Mockito.any(DalHints.class), Mockito.anyList());
        Mockito.verify(applierGroupTblV2Dao, times(2)).batchUpdate(Mockito.anyList());
        Assert.assertEquals(result.getInsertSize(), 9);
        Assert.assertEquals(result.getUpdateSize(), 1);
    }

    @Test
    public void testCheckMhaDbMapping() throws Exception {
        initMhaDbMapping();
        MhaDbMappingResult result = migrationService.checkMhaDbMapping();
        Assert.assertTrue(CollectionUtils.isEmpty(result.getNotExistMhaNames()));
        Assert.assertTrue(CollectionUtils.isEmpty(result.getNotExistDbNames()));
    }

    @Test
    public void testMigrateMhaDbMapping() throws Exception {
        initMhaDbMapping();
        Mockito.when(mhaDbMappingTblDao.batchInsert(Mockito.anyList())).thenReturn(new int[0]);
        MigrateMhaDbMappingResult result = migrationService.migrateMhaDbMapping();
        Assert.assertEquals(result.getTotal(), 1);
        Assert.assertTrue(CollectionUtils.isEmpty(result.getNotExistMhaNames()));
    }

    @Test
    public void testCheckMhaFilter() throws Exception{
        Mockito.when(dataMediaTblDao.queryByAGroupId(Mockito.anyLong(), Mockito.anyInt())).thenReturn(Lists.newArrayList(getDataMediaTbl()));
        Mockito.when(dataMediaTblDao.queryByIdsAndType(Mockito.anyList(), Mockito.anyInt(), Mockito.anyInt())).thenReturn(Lists.newArrayList(getDataMediaTbl()));
        Mockito.when(rowsFilterMappingTblDao.queryAll()).thenReturn(MigrateEntityBuilder.getRowsFilterMapping());

        List<MhaNameFilterVo> mhaNameFilterVos = migrationService.checkMhaFilter();
//        mhaNameFilterVos.forEach(System.out::println);
        Assert.assertEquals(mhaNameFilterVos.size(), 9);
    }

    @Test
    public void testSplitNameFilter() throws Exception {
        List<NameFilterSplitParam> params = MigrateEntityBuilder.getApplierGroupTbls().stream().map(source -> {
            NameFilterSplitParam target = new NameFilterSplitParam();
            target.setApplierGroupId(source.getId());
            target.setMhaName("mha");
            target.setNameFilter("nameFilter");
            return target;
        }).collect(Collectors.toList());
        MySqlUtils.TableSchemaName table = new MySqlUtils.TableSchemaName("test", "table");
        TableCheckVo vo = new TableCheckVo(table);

        try(MockedStatic<MySqlUtils> theMock = Mockito.mockStatic(MySqlUtils.class)) {
            theMock.when(() -> MySqlUtils.checkTablesWithFilter(Mockito.any(),Mockito.anyString())).thenReturn(Lists.newArrayList(vo));
            Mockito.when(dbClusterSourceProvider.getMasterEndpoint(Mockito.anyString()))
                    .thenReturn(new MySqlEndpoint("ip", 20, "user", "pwd", true));
            int result = migrationService.splitNameFilter(params);
            Assert.assertEquals(result, params.size());
        }
    }

    @Test
    public void testCheckNameMapping() throws Exception {
        List<String> result = migrationService.checkNameMapping();
        Assert.assertEquals(result.size(), MigrateEntityBuilder.getApplierGroupTbls().size());
    }

    @Test
    public void testSplitNameFilterWithNameMapping() throws Exception {
        MySqlUtils.TableSchemaName table = new MySqlUtils.TableSchemaName("test", "table");
        TableCheckVo vo = new TableCheckVo(table);

        try(MockedStatic<MySqlUtils> theMock = Mockito.mockStatic(MySqlUtils.class)) {
            theMock.when(() -> MySqlUtils.checkTablesWithFilter(Mockito.any(),Mockito.anyString())).thenReturn(Lists.newArrayList(vo));
            Mockito.when(dbClusterSourceProvider.getMasterEndpoint(Mockito.anyString()))
                    .thenReturn(new MySqlEndpoint("ip", 20, "user", "pwd", true));
            Mockito.when(applierGroupTblDao.queryAll()).thenReturn(Lists.newArrayList(getApplierGroup()));
            int result = migrationService.splitNameFilterWithNameMapping();
            Assert.assertEquals(result, 1);
        }
    }

    private void initMhaDbMapping() throws Exception {
        List<MhaTbl> mhaTblList = MigrateEntityBuilder.getMhaTbls().stream().filter(e -> e.getId().equals(100L)).collect(Collectors.toList());
        Mockito.when(mhaTblDao.queryAll()).thenReturn(mhaTblList);
        Mockito.when(dbTblDao.queryAll()).thenReturn(getDbTbls());

        Map<String, Map<String, List<String>>> dbNameMap = new HashMap<>();
        Map<String, List<String>> dbMap = new HashMap<>();
        dbMap.put("mha100", Lists.newArrayList("db"));
        dbNameMap.put("mha100", dbMap);
        Mockito.when(dalService.getDbNames(Mockito.anyList(), Mockito.any(Env.class))).thenReturn(dbNameMap);
    }

    private DataMediaTbl getDataMediaTbl() {
        DataMediaTbl dataMediaTbl = new DataMediaTbl();
        dataMediaTbl.setDeleted(0);
        dataMediaTbl.setNamespcae("testDb");
        dataMediaTbl.setName("table100");
        return dataMediaTbl;
    }

    private ApplierGroupTbl getApplierGroup() {
        ApplierGroupTbl applierGroupTbl = new ApplierGroupTbl();
        applierGroupTbl.setDeleted(0);
        applierGroupTbl.setId(100L);
        applierGroupTbl.setReplicatorGroupId(100L);
        applierGroupTbl.setMhaId(100L);
        applierGroupTbl.setNameFilter("drcmonitordb\\.delaymonitor,testDb\\.table");
        applierGroupTbl.setNameMapping("test.db,test.db1");
        return applierGroupTbl;
    }


}
