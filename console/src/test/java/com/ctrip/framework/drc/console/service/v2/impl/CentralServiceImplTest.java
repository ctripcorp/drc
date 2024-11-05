package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorTbl;
import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.param.MhaReplicatorEntity;
import com.ctrip.framework.drc.console.service.v2.MachineService;
import com.ctrip.framework.drc.console.service.v2.MhaDbReplicationService;
import java.sql.SQLException;
import java.util.List;

import com.ctrip.framework.drc.console.service.v2.MockEntityBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class CentralServiceImplTest {
    @InjectMocks
    private CentralServiceImpl centralServiceImpl;
    @Mock
    private MhaTblV2Dao mhaTblV2Dao;
    @Mock
    private DcTblDao dcTblDao;
    @Mock
    private DdlHistoryTblDao ddlHistoryTblDao;
    @Mock
    private MhaDbReplicationService mhaDbReplicationService;
    @Mock
    private MachineService machineService;
    @Mock private ReplicatorGroupTblDao rGroupTblDao;

    @Mock private ReplicatorTblDao replicatorTblDao;

    @Mock private ResourceTblDao resourceTblDao;
    
    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testGetUuidInMetaDb() throws SQLException {
        Mockito.when(machineService.getUuid(Mockito.anyString(), Mockito.anyInt())).thenReturn("test");
        Assert.assertEquals("test", centralServiceImpl.getUuidInMetaDb("test", "ip",1));
    }

    @Test
    public void testCorrectMachineUuid() throws SQLException {
        Mockito.when(machineService.correctUuid(Mockito.anyString(),Mockito.anyInt(),Mockito.anyString())).thenReturn(1);
        MachineTbl machineTbl = new MachineTbl();
        machineTbl.setIp("ip");
        machineTbl.setUuid("uuid");
        machineTbl.setPort(1);
        Assert.assertEquals(1, centralServiceImpl.correctMachineUuid(machineTbl).intValue());
    }

    @Test
    public void testUpdateMasterReplicatorIfChange() throws SQLException {
        MhaTblV2 mhaTblV2 = MockEntityBuilder.buildMhaTblV2();
        ReplicatorGroupTbl rGroupTbl = MockEntityBuilder.buildReplicatorGroupTbl();
        List<ReplicatorTbl> replicatorTbls = MockEntityBuilder.buildReplicatorTbls();
        ResourceTbl resourceTbl1 = MockEntityBuilder.buildResourceTbl();
        resourceTbl1.setIp("ip1");
        ResourceTbl resourceTbl2 = MockEntityBuilder.buildResourceTbl();
        resourceTbl2.setIp("ip2");
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq("mha"))).thenReturn(mhaTblV2);
        Mockito.when(rGroupTblDao.queryByMhaId(Mockito.eq(1L))).thenReturn(rGroupTbl);
        Mockito.when(replicatorTblDao.queryByRGroupIds(Mockito.anyList(),Mockito.anyInt())).thenReturn(replicatorTbls);
        Mockito.when(resourceTblDao.queryByPk(Mockito.eq(1L))).thenReturn(resourceTbl1);
        Mockito.when(resourceTblDao.queryByPk(Mockito.eq(2L))).thenReturn(resourceTbl2);
        Mockito.when(replicatorTblDao.batchUpdate(Mockito.anyList())).thenReturn(new int[] {1,1});

        boolean b = centralServiceImpl.updateMasterReplicatorIfChange(new MhaReplicatorEntity("mha", "ip2"));
        boolean b1 = centralServiceImpl.updateMasterReplicatorIfChange(new MhaReplicatorEntity("mha", "ip2"));
        Assert.assertTrue(b);
        Assert.assertFalse(b1);
    }

}