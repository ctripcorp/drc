package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.MachineTblDao;
import com.ctrip.framework.drc.console.dao.ReplicatorGroupTblDao;
import com.ctrip.framework.drc.console.dao.ReplicatorTblDao;
import com.ctrip.framework.drc.console.dao.ResourceTblDao;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorTbl;
import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto.MySQLInstance;
import com.ctrip.framework.drc.console.enums.DrcAccountTypeEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.param.v2.security.Account;
import com.ctrip.framework.drc.console.service.v2.MockEntityBuilder;
import com.ctrip.framework.drc.console.service.v2.security.AccountService;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.core.http.ApiResult;
import java.sql.SQLException;
import java.util.List;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class DbMetaCorrectServiceImplTest {

    @InjectMocks private DbMetaCorrectServiceImpl dbMetaCorrectService;

    @Mock private MhaTblV2Dao mhaTblV2Dao;

    @Mock private ReplicatorGroupTblDao rGroupTblDao;

    @Mock private ReplicatorTblDao replicatorTblDao;

    @Mock private ResourceTblDao resourceTblDao;

    @Mock private MachineTblDao machineTblDao;

    @Mock private MonitorTableSourceProvider monitorTableSourceProvider;
    
    @Mock private AccountService accountService;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        Mockito.when(accountService.getAccount(Mockito.any(MhaTblV2.class), Mockito.any(DrcAccountTypeEnum.class))).thenReturn(new Account("user", "password"));
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

        boolean b = dbMetaCorrectService.updateMasterReplicatorIfChange("mha", "ip2");
        boolean b1 = dbMetaCorrectService.updateMasterReplicatorIfChange("mha", "ip2");
        Assert.assertTrue(b);
        Assert.assertFalse(b1);
    }

    @Test
    public void testMhaInstancesChange() throws Exception {
        //init Mock
        int[] effects = new int[]{1,1};
        Mockito.when(machineTblDao.batchUpdate(Mockito.anyList())).thenReturn(effects);
        Mockito.when(machineTblDao.batchLogicalDelete(Mockito.anyList())).thenReturn(effects);
        Mockito.when(machineTblDao.batchInsert(Mockito.anyList())).thenReturn(effects);
        Mockito.when(monitorTableSourceProvider.getSwitchSyncMhaUpdateAll()).thenReturn("on");

        //init dto
        MhaInstanceGroupDto dto = new MhaInstanceGroupDto();
        MhaInstanceGroupDto.MySQLInstance master = new MhaInstanceGroupDto.MySQLInstance();
        master.setIp("ip1");
        master.setPort(3306);
        dto.setMaster(master);

        MhaInstanceGroupDto.MySQLInstance slave1 = new MhaInstanceGroupDto.MySQLInstance();
        slave1.setIp("ip2");
        slave1.setPort(3307);

        MhaInstanceGroupDto.MySQLInstance slave2 = new MhaInstanceGroupDto.MySQLInstance();
        slave2.setIp("ip3");
        slave2.setPort(3308);

        List<MySQLInstance> slaves = Lists.newArrayList();
        slaves.add(slave1);
        slaves.add(slave2);
        dto.setSlaves(slaves);

        //init tbl
        MhaTblV2 mhaTbl = new MhaTblV2();
        mhaTbl.setId(1L);
        mhaTbl.setMhaName("mhaName");
        mhaTbl.setMonitorUser("monitorUser");
        mhaTbl.setMonitorPassword("monitorPsw");


        // test
        try(MockedStatic<MySqlUtils> theMock = Mockito.mockStatic(MySqlUtils.class)){
            theMock.when(() ->MySqlUtils.getUuid(
                    Mockito.anyString(),Mockito.anyInt(),Mockito.anyString(),Mockito.anyString(),Mockito.anyBoolean())
            ).thenReturn("uuid");

            //case1 update only
            List<MachineTbl> machinesInMetaDb = Lists.newArrayList();
            MachineTbl machineInMetaDb1 = new MachineTbl("ip1", 3306, 0);
            MachineTbl machineInMetaDb2 = new MachineTbl("ip2", 3307, 1);
            MachineTbl machineInMetaDb3 = new MachineTbl("ip3", 3308, 0);
            machinesInMetaDb.add(machineInMetaDb1);
            machinesInMetaDb.add(machineInMetaDb2);
            machinesInMetaDb.add(machineInMetaDb3);
            Mockito.when(machineTblDao.queryByMhaId(Mockito.eq(1L),Mockito.eq(1))).thenReturn(machinesInMetaDb);


            final List<MachineTbl> insertMachines = com.google.common.collect.Lists.newArrayList();
            final List<MachineTbl> deleteMachines = com.google.common.collect.Lists.newArrayList();
            final List<MachineTbl> updateMachines = com.google.common.collect.Lists.newArrayList();
            dbMetaCorrectService.checkChange(
                    dto,machinesInMetaDb,mhaTbl,insertMachines,updateMachines,deleteMachines
            );
            Assert.assertEquals(0,insertMachines.size());
            Assert.assertEquals(2,updateMachines.size());
            Assert.assertEquals(0,deleteMachines.size());


            //case2 delete only
            machinesInMetaDb = Lists.newArrayList();
            machineInMetaDb1 = new MachineTbl("ip1", 3306, 1);
            machineInMetaDb2 = new MachineTbl("ip2", 3307, 0);
            machineInMetaDb3 = new MachineTbl("ip3", 3308, 0);
            MachineTbl machineInMetaDb4 = new MachineTbl("ip4", 3308, 0);
            machinesInMetaDb.add(machineInMetaDb1);
            machinesInMetaDb.add(machineInMetaDb2);
            machinesInMetaDb.add(machineInMetaDb3);
            machinesInMetaDb.add(machineInMetaDb4);

            insertMachines.clear();
            deleteMachines.clear();
            updateMachines.clear();
            dbMetaCorrectService.checkChange(
                    dto,machinesInMetaDb,mhaTbl,insertMachines,updateMachines,deleteMachines
            );
            Assert.assertEquals(0,insertMachines.size());
            Assert.assertEquals(0,updateMachines.size());
            Assert.assertEquals(1,deleteMachines.size());



            //case3 insert only
            machinesInMetaDb = Lists.newArrayList();
            machineInMetaDb1 = new MachineTbl("ip1", 3306, 1);
            machineInMetaDb2 = new MachineTbl("ip2", 3307, 0);
            machinesInMetaDb.add(machineInMetaDb1);
            machinesInMetaDb.add(machineInMetaDb2);

            insertMachines.clear();
            deleteMachines.clear();
            updateMachines.clear();
            dbMetaCorrectService.checkChange(
                    dto,machinesInMetaDb,mhaTbl,insertMachines,updateMachines,deleteMachines
            );
            Assert.assertEquals(1,insertMachines.size());
            Assert.assertEquals(0,updateMachines.size());
            Assert.assertEquals(0,deleteMachines.size());


            //case4 update,insert,delete
            machinesInMetaDb = Lists.newArrayList();
            machineInMetaDb1 = new MachineTbl("ip1", 3306, 0);
            machineInMetaDb2 = new MachineTbl("ip2", 3307, 1);
            machineInMetaDb3 = new MachineTbl("ip4", 3308, 0);
            machinesInMetaDb.add(machineInMetaDb1);
            machinesInMetaDb.add(machineInMetaDb2);
            machinesInMetaDb.add(machineInMetaDb3);

            insertMachines.clear();
            deleteMachines.clear();
            updateMachines.clear();
            dbMetaCorrectService.checkChange(
                    dto,machinesInMetaDb,mhaTbl,insertMachines,updateMachines,deleteMachines
            );
            Assert.assertEquals(1,insertMachines.size());
            Assert.assertEquals(2,updateMachines.size());
            Assert.assertEquals(1,deleteMachines.size());

            dbMetaCorrectService.mhaInstancesChange(dto,mhaTbl);
        }

        //test validate 
        try {
            dto.setMaster(null);
            dto.transferToMachine();
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
        }


    }

    @Test
    public void testMhaMasterDbChange() throws Exception {
        MhaTblV2 mhaTblV2 = MockEntityBuilder.buildMhaTblV2();
        List<MachineTbl> machineTbls = MockEntityBuilder.buildMachineTbls();

        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq("mha"))).thenReturn(mhaTblV2);
        Mockito.when(machineTblDao.queryByMhaId(Mockito.eq(1L),Mockito.eq(0))).thenReturn(machineTbls);
        Mockito.when(machineTblDao.batchUpdate(Mockito.anyList())).thenReturn(new int[] {1,1});

        List<MachineTbl> machineTblToBeUpdated = dbMetaCorrectService.checkMachinesInUse(1L, "mha", "ip2", 2);
        Assert.assertEquals(2,machineTblToBeUpdated.size());

        ApiResult apiResult = dbMetaCorrectService.mhaMasterDbChange("mha", "ip2", 2);
        Assert.assertEquals(0,apiResult.getData());
        apiResult = dbMetaCorrectService.mhaMasterDbChange("mha", "ip1", 1);
        Assert.assertEquals(2,apiResult.getData());
    }

    @Test
    public void testMhaInstancesChange2() throws Exception {
        //init Mock
        int[] effects = new int[]{1,1};
        Mockito.when(machineTblDao.batchUpdate(Mockito.anyList())).thenReturn(effects);
        Mockito.when(machineTblDao.batchLogicalDelete(Mockito.anyList())).thenReturn(effects);
        Mockito.when(machineTblDao.batchInsert(Mockito.anyList())).thenReturn(effects);
        Mockito.when(monitorTableSourceProvider.getSwitchSyncMhaUpdateAll()).thenReturn("on");

        //init dto

        List<MachineTbl> machineTblFromDal = Lists.newArrayList();
        machineTblFromDal.add(new MachineTbl("ip1",3306,1));
        machineTblFromDal.add(new MachineTbl("ip2",3307,0));
        machineTblFromDal.add(new MachineTbl("ip3",3308,0));

        //init tbl
        MhaTblV2 mhaTbl = new MhaTblV2();
        mhaTbl.setId(1L);
        mhaTbl.setMhaName("mhaName");
        mhaTbl.setMonitorUser("monitorUser");
        mhaTbl.setMonitorPassword("monitorPsw");


        // test
        try (MockedStatic<MySqlUtils> theMock = Mockito.mockStatic(MySqlUtils.class)) {
            theMock.when(() -> MySqlUtils.getUuid(
                    Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean())
            ).thenReturn("uuid");
            final List<MachineTbl> insertMachines = com.google.common.collect.Lists.newArrayList();
            final List<MachineTbl> deleteMachines = com.google.common.collect.Lists.newArrayList();
            final List<MachineTbl> updateMachines = com.google.common.collect.Lists.newArrayList();


            // case1: update only
            List<MachineTbl> machinesInMetaDb = Lists.newArrayList();
            machinesInMetaDb.add(new MachineTbl("ip1", 3306, 0));
            machinesInMetaDb.add(new MachineTbl("ip2", 3307, 1));
            machinesInMetaDb.add(new MachineTbl("ip3", 3308, 0));
            Mockito.when(machineTblDao.queryByMhaId(Mockito.eq(1L), Mockito.eq(1))).thenReturn(machinesInMetaDb);


            dbMetaCorrectService.checkChange(machineTblFromDal, machinesInMetaDb, mhaTbl, insertMachines, updateMachines, deleteMachines);
            Assert.assertEquals(0, insertMachines.size());
            Assert.assertEquals(2, updateMachines.size());
            Assert.assertEquals(0, deleteMachines.size());
            // case2: insert
            insertMachines.clear();
            deleteMachines.clear();
            updateMachines.clear();
            machinesInMetaDb.clear();

            machinesInMetaDb.add(new MachineTbl("ip1", 3306, 1));
            dbMetaCorrectService.checkChange(machineTblFromDal, machinesInMetaDb, mhaTbl, insertMachines, updateMachines, deleteMachines);
            Assert.assertEquals(2, insertMachines.size());
            Assert.assertEquals(0, updateMachines.size());
            Assert.assertEquals(0, deleteMachines.size());

            // case3: delete
            insertMachines.clear();
            deleteMachines.clear();
            updateMachines.clear();
            machinesInMetaDb.clear();

            machinesInMetaDb.add(new MachineTbl("ip1", 3306, 1));
            machinesInMetaDb.add(new MachineTbl("ip2", 3307, 0));
            machinesInMetaDb.add(new MachineTbl("ip3", 3308, 0));
            machinesInMetaDb.add(new MachineTbl("ip4", 3309, 0));
            dbMetaCorrectService.checkChange(machineTblFromDal, machinesInMetaDb, mhaTbl, insertMachines, updateMachines, deleteMachines);
            Assert.assertEquals(0, insertMachines.size());
            Assert.assertEquals(0, updateMachines.size());
            Assert.assertEquals(1, deleteMachines.size());


            // case4: mixed
            insertMachines.clear();
            deleteMachines.clear();
            updateMachines.clear();
            machinesInMetaDb.clear();

            machinesInMetaDb.add(new MachineTbl("ip1", 3306, 0));
            machinesInMetaDb.add(new MachineTbl("ip2", 3307, 0));
            machinesInMetaDb.add(new MachineTbl("ip4", 3309, 1));
            dbMetaCorrectService.checkChange(machineTblFromDal, machinesInMetaDb, mhaTbl, insertMachines, updateMachines, deleteMachines);
            Assert.assertEquals(1, insertMachines.size());
            Assert.assertEquals(1, updateMachines.size());
            Assert.assertEquals(1, deleteMachines.size());

            dbMetaCorrectService.mhaInstancesChange(machineTblFromDal, mhaTbl);
        }
    }
}