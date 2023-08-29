package com.ctrip.framework.drc.console.service.v2.dbmigration.impl;

import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.MachineTblDao;
import com.ctrip.framework.drc.console.dao.MessengerGroupTblDao;
import com.ctrip.framework.drc.console.dao.MessengerTblDao;
import com.ctrip.framework.drc.console.dao.ReplicatorGroupTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaDbMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.ApplierGroupTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.ApplierTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationFilterMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaDbMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.MigrationTaskTblDao;
import com.ctrip.framework.drc.console.dto.v2.DbMigrationParam;
import com.ctrip.framework.drc.console.dto.v2.DbMigrationParam.MigrateMhaInfo;
import com.ctrip.framework.drc.console.service.v2.DrcBuildServiceV2;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.MhaDbMappingService;
import com.ctrip.framework.drc.console.service.v2.MockEntityBuilder;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.service.v2.impl.MetaGeneratorV3;
import com.google.common.collect.Lists;
import java.sql.SQLException;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class DbMigrationServiceImplTest {
    
    @InjectMocks
    private DbMigrationServiceImpl dbMigrationService;
    @Mock
    private MetaInfoServiceV2 metaInfoServiceV2;
    @Mock
    private MachineTblDao machineTblDao;
    @Mock
    private MigrationTaskTblDao migrationTaskTblDao;
    @Mock
    private MhaTblV2Dao mhaTblV2Dao;
    @Mock
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Mock
    private ApplierGroupTblV2Dao applierGroupTblV2Dao;
    @Mock
    private ApplierTblV2Dao applierTblV2Dao;
    @Mock
    private MhaReplicationTblDao mhaReplicationTblDao;
    @Mock
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Mock
    private DbReplicationTblDao dbReplicationTblDao;
    @Mock
    private DbReplicationFilterMappingTblDao dbReplicationFilterMappingTblDao;
    @Mock
    private DbTblDao dbTblDao;
    @Mock
    private MessengerGroupTblDao messengerGroupTblDao;
    @Mock
    private MessengerTblDao messengerTblDao;
    @Mock
    private MetaGeneratorV3 metaGeneratorV3;
    @Mock
    private DcTblDao dcTblDao;
    @Mock
    private MhaDbMappingService mhaDbMappingService;
    @Mock
    private DrcBuildServiceV2 drcBuildServiceV2;
    @Mock
    private MysqlServiceV2 mysqlServiceV2;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        
        
    }

    @Test
    public void testDbMigrationCheckAndCreateTask() throws SQLException {
        DbMigrationParam dbMigrationParam = mockDbMigrationParam();
        
        
        
    }

    @Test
    public void testExStartDbMigrationTask() {
    }

    @Test
    public void testStartDbMigrationTask() {
    }
    
    private void initMocks() throws SQLException{
        DbMigrationParam dbMigrationParam = mockDbMigrationParam();
        // check and init mha info
        MachineTbl machineTbl1 = MockEntityBuilder.buildMachineTbl();
        MhaTblV2 oldMhaTbl = MockEntityBuilder.buildMhaTblV2();
        oldMhaTbl.setId(1L);
        oldMhaTbl.setMhaName("mha1");
        Mockito.when(machineTblDao.queryByIpPort(Mockito.eq("ip1"), Mockito.eq(3306))).thenReturn(machineTbl1);
        Mockito.when(mhaTblV2Dao.queryByPk(Mockito.eq(1L))).thenReturn(oldMhaTbl);

        MhaTblV2 newMhaTbl = MockEntityBuilder.buildMhaTblV2();
        newMhaTbl.setId(2L);
        newMhaTbl.setMhaName("mha2");
        Mockito.when(machineTblDao.queryByIpPort(Mockito.eq("ip2"), Mockito.eq(3306))).thenReturn(null);
        Mockito.when(drcBuildServiceV2.syncMhaInfoFormDbaApi(Mockito.eq("mha2"))).thenReturn(MockEntityBuilder.buildMhaTblV2());
        
        // init tblEntitys 
        

        // getReplicationInfoInOldMha
        List<DbTbl> dbTbls = MockEntityBuilder.buildDbTblList(2);
        Mockito.when(dbTblDao.queryByDbNames(Mockito.eq(dbMigrationParam.getDbs()))).thenReturn(dbTbls);
        List<MhaDbMappingTbl> mhaDbMappingTbls = MockEntityBuilder.buildMhaDbMappings(2, 1L);
        Mockito.when(mhaDbMappingTblDao.queryByDbIdAndMhaId(Mockito.eq(1L),Mockito.eq(1L))).thenReturn(mhaDbMappingTbls.get(0));
        Mockito.when(mhaDbMappingTblDao.queryByDbIdAndMhaId(Mockito.eq(2L),Mockito.eq(1L))).thenReturn(mhaDbMappingTbls.get(1));
        
        // db1
        Mockito.when(dbReplicationTblDao.queryBySrcMappingIds(Mockito.eq(Lists.newArrayList(1L)),Mockito.eq(0)))
                .thenReturn(MockEntityBuilder.buildDbReplicationTblListBySrc(1, 1L));
//        Mockito.when(mhaDbMappingTblDao.queryByIds(Mockito.eq(Lists.newArrayList(1L)))).thenReturn();
//        Mockito.when(mhaTblV2Dao.queryByIds(Mockito.eq(Lists.newArrayList(1L)))).thenReturn();
        Mockito.when(dbReplicationTblDao.queryByDestMappingIds(Mockito.eq(Lists.newArrayList(1L)),Mockito.eq(0)))
                .thenReturn(Lists.newArrayList());
        Mockito.when(dbReplicationTblDao.queryByDestMappingIds(Mockito.eq(Lists.newArrayList(1L)),Mockito.eq(1)))
                .thenReturn(Lists.newArrayList());
        
        
        // db2
    }
    
    private DbMigrationParam mockDbMigrationParam() {
        // new DbMigrationParam set all fields
        DbMigrationParam dbMigrationParam = new DbMigrationParam();
        dbMigrationParam.setOperator("operator");
        dbMigrationParam.setDbs(Lists.newArrayList("db1", "db2"));
        
        MigrateMhaInfo migrateMhaInfo = new MigrateMhaInfo();
        migrateMhaInfo.setName("mha1");
        migrateMhaInfo.setMasterIp("ip1");
        migrateMhaInfo.setMasterPort(3306);
        dbMigrationParam.setOldMha(migrateMhaInfo);

        MigrateMhaInfo migrateMhaInfo1 = new MigrateMhaInfo();
        migrateMhaInfo1.setName("mha2");
        migrateMhaInfo1.setMasterIp("ip2");
        migrateMhaInfo1.setMasterPort(3306);
        dbMigrationParam.setNewMha(migrateMhaInfo1);
        
        return dbMigrationParam; 
    }
}