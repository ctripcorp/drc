package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaDbMappingTbl;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.service.DrcBuildService;
import com.ctrip.framework.drc.console.service.v2.impl.DrcDoubleWriteServiceImpl;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.service.v2.MigrateEntityBuilder.*;

/**
 * Created by dengquanliang
 * 2023/7/19 19:45
 */
public class DrcDoubleWriteServiceTest {
    
    @InjectMocks
    private DrcDoubleWriteServiceImpl drcDoubleWriteService;
    @Mock
    private MhaTblV2Dao mhaTblV2Dao;
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
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Mock
    private DrcBuildService drcBuildService;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testConfigureMhaReplication() throws Exception {
        List<MhaDbMappingTbl> mhaDbMappingTbls = getMhaDbMappingTbls().stream().filter(e -> e.getMhaId() == 200L).collect(Collectors.toList());
        Mockito.when(applierGroupTblDao.queryById(Mockito.anyLong())).thenReturn(getApplierGroupTbl());
        Mockito.when(replicatorGroupTblDao.queryById(Mockito.anyLong())).thenReturn(getReplicatorGroupTbl());
        Mockito.when(replicatorGroupTblDao.queryByMhaId(Mockito.anyLong())).thenReturn(getReplicatorGroupTbl());
        Mockito.when(applierGroupTblDao.queryByMhaIdAndReplicatorGroupId(Mockito.anyLong(), Mockito.anyLong())).thenReturn(getApplierGroupTbl());
        Mockito.when(applierGroupTblV2Dao.queryByPk(Mockito.anyLong())).thenReturn(getApplierGroupTblV2s().get(0));
        Mockito.when(applierGroupTblV2Dao.queryById(Mockito.anyLong())).thenReturn(getApplierGroupTblV2s().get(0));
        Mockito.when(applierTblV2Dao.queryByApplierGroupId(Mockito.anyLong())).thenReturn(getApplierTblV2s());
        Mockito.when(mhaDbMappingTblDao.queryByMhaId(Mockito.anyLong())).thenReturn(mhaDbMappingTbls);
        Mockito.when(mhaReplicationTblDao.queryByMhaId(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyInt())).thenReturn(getMhaReplicationTbl());
        Mockito.when(dbReplicationTblDao.queryByMappingIds(Mockito.anyList(), Mockito.anyList(), Mockito.anyInt())).thenReturn(getDbReplicationTbls());
        Mockito.when(applierTblDao.queryByApplierGroupIds(Mockito.anyList(), Mockito.anyInt())).thenReturn(getApplierTbls());
        Mockito.when(mhaTblV2Dao.queryById(Mockito.anyLong())).thenReturn(getMhaTblV2());
        Mockito.when(dbTblDao.queryByDbNames(Mockito.anyList())).thenReturn(getDbTbls());
        Mockito.when(drcBuildService.queryDbsWithNameFilter(Mockito.anyString(), Mockito.anyString())).thenReturn(Lists.newArrayList("db200"));


        drcDoubleWriteService.configureMhaReplication(200L);
    }

}
