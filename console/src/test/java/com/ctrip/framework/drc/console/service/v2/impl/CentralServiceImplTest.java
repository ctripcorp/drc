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
import com.ctrip.framework.drc.console.service.v2.MockEntityBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.getMhaTblV2s;
import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.getReplicatorGroupTbls;

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

    @Test
    public void testBatchUpdateMasterReplicatorIfChange() throws SQLException {
        Mockito.when(mhaTblV2Dao.queryAllExist()).thenReturn(getMhaTblV2s());
        Mockito.when(rGroupTblDao.queryAllExist()).thenReturn(getReplicatorGroupTbls());
        Mockito.when(replicatorTblDao.queryAllExist()).thenReturn(getReplicatorTbls());
        Mockito.when(resourceTblDao.queryAllExist()).thenReturn(getResourceTbls());

        Map<String, String> map = new HashMap<>();
        map.put("mha200", "ip200");
        centralServiceImpl.batchUpdateMasterReplicatorIfChange(new MhaReplicatorEntity(map));
        Mockito.verify(replicatorTblDao, Mockito.never()).batchUpdate(Mockito.anyList());

        map.put("mha200", "ip201");
        centralServiceImpl.batchUpdateMasterReplicatorIfChange(new MhaReplicatorEntity(map));
        Mockito.verify(replicatorTblDao, Mockito.times(1)).batchUpdate(Mockito.anyList());
    }

    public static List<ReplicatorTbl> getReplicatorTbls() {
        List<ReplicatorTbl> tbls = new ArrayList<>();
        for (int i = 200; i < 202; i++) {
            ReplicatorTbl replicatorTbl = new ReplicatorTbl();
            replicatorTbl.setId(Long.valueOf(i));
            replicatorTbl.setDeleted(0);
            replicatorTbl.setRelicatorGroupId(200L);
            replicatorTbl.setResourceId(replicatorTbl.getId());
            replicatorTbl.setApplierPort(1010);
            replicatorTbl.setGtidInit("gtId");
            replicatorTbl.setPort(3030);
            if (i == 200) {
                replicatorTbl.setMaster(1);
            } else {
                replicatorTbl.setMaster(0);
            }
            tbls.add(replicatorTbl);
        }

        return tbls;
    }

    public static List<ResourceTbl> getResourceTbls() {
        List<ResourceTbl> tbls = new ArrayList<>();

        for (int i = 200; i < 202; i++) {
            ResourceTbl resourceTbl = new ResourceTbl();
            resourceTbl.setId(Long.valueOf(i));
            resourceTbl.setType(0);
            resourceTbl.setAz("AZ");
            resourceTbl.setIp("ip" + i);
            resourceTbl.setTag("tag");
            resourceTbl.setDcId(200L);
            tbls.add(resourceTbl);
        }
        return tbls;
    }

}