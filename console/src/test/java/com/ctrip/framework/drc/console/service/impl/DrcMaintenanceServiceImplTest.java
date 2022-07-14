package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.AbstractTest;
import com.ctrip.framework.drc.console.dao.MachineTblDao;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.EstablishStatusEnum;
import com.ctrip.framework.drc.console.enums.TableEnum;
import com.ctrip.framework.drc.console.monitor.DefaultCurrentMetaManager;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.MhaGroupPair;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;
import static com.ctrip.framework.drc.console.service.impl.MetaGeneratorTest.*;
import static org.mockito.Mockito.doNothing;

public class DrcMaintenanceServiceImplTest extends AbstractTest {

    public static final String MHA1OY = "fat-fx-drc1";

    public static final String MHA1RB = "fat-fx-drc2";

    public static final String MHA2OY = "drcTestW1";

    public static final String MHA2RB = "drcTestW2";

    private DalUtils dalUtils = DalUtils.getInstance();

    @InjectMocks
    private DrcMaintenanceServiceImpl drcMaintenanceService;

    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;
    @Mock
    private MetaInfoServiceImpl metaInfoService;

    @Mock
    private DefaultCurrentMetaManager currentMetaManager;
    
    @Mock
    private MachineTblDao machineTblDao;
            
    private MetaInfoServiceImpl metaInfoServiceImpl = new MetaInfoServiceImpl();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        doNothing().when(currentMetaManager).addSlaveMySQL(Mockito.anyString(), Mockito.any());
        MhaGroupTbl mhaGroupTbl = dalUtils.getMhaGroupTblDao().queryByPk(1L);
        Mockito.doReturn(mhaGroupTbl).when(metaInfoService).getMhaGroupForMha(MHA1OY);
        Mockito.doReturn(1L).when(metaInfoService).getMhaGroupId(MHA1OY, MHA1RB);
        Mockito.doReturn(mhaGroupTbl).when(metaInfoService).getMhaGroup(MHA1OY, MHA1RB);
        Mockito.doReturn("off").when(monitorTableSourceProvider).getSlaveMachineOfflineSyncSwitch();
    }

    @Test
    public void recordMhaInstances() throws Throwable {
        MhaInstanceGroupDto dto = new MhaInstanceGroupDto();
        dto.setMhaName("fat-fx-drc2");
        MhaInstanceGroupDto.MySQLInstance mySQLInstance = new MhaInstanceGroupDto.MySQLInstance();
        mySQLInstance.setIp("10.2.72.246");
        mySQLInstance.setPort(55111);
        dto.setMaster(mySQLInstance);
        Boolean isSuccess = drcMaintenanceService.recordMhaInstances(dto);
        Assert.assertEquals(true,isSuccess);
    }
    
    @Test
    public void testChangeMasterDb() {
        try(MockedStatic<MySqlUtils> theMock = Mockito.mockStatic(MySqlUtils.class)) {
            theMock.when(() -> MySqlUtils.getUuid(Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean())).thenReturn("uuid");

            ApiResult result = drcMaintenanceService.changeMasterDb("nosuchmha", "127.0.0.1", 3306);
            Assert.assertEquals(ResultCode.HANDLE_FAIL.getCode(), result.getStatus().intValue());
            Assert.assertTrue(result.getMessage().contains("no such mha"));

            result = drcMaintenanceService.changeMasterDb("fat-fx-drc1", "10.2.72.247", 55111);
            Assert.assertEquals(ResultCode.HANDLE_SUCCESS.getCode(), result.getStatus().intValue());
            Assert.assertEquals(2, ((Integer) result.getData()).intValue());
            Assert.assertEquals("update fat-fx-drc1 master instance succeeded, u2i0", result.getMessage());

            result = drcMaintenanceService.changeMasterDb("fat-fx-drc1", "10.2.72.247", 55111);
            Assert.assertEquals(ResultCode.HANDLE_SUCCESS.getCode(), result.getStatus().intValue());
            Assert.assertEquals(0, ((Integer) result.getData()).intValue());
            Assert.assertEquals("fat-fx-drc1 10.2.72.247:55111 already master", result.getMessage());


            theMock.when(() -> MySqlUtils.getUuid(Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean())).thenThrow(new Exception("getUuidError"));
            result = drcMaintenanceService.changeMasterDb("fat-fx-drc1", "10.2.72.555", 55111);
            Assert.assertEquals(ResultCode.HANDLE_FAIL.getCode(), result.getStatus().intValue());
            Assert.assertEquals(0, ((Integer) result.getData()).intValue());
            Assert.assertTrue(result.getMessage().contains("Fail update master instance as"));


            theMock.when(() -> MySqlUtils.getUuid(Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean())).thenReturn("uuid");
            result = drcMaintenanceService.changeMasterDb("fat-fx-drc1", "10.2.72.248", 55111);
            Assert.assertEquals(ResultCode.HANDLE_SUCCESS.getCode(), result.getStatus().intValue());
            Assert.assertEquals(2, ((Integer) result.getData()).intValue());
            Assert.assertEquals("update fat-fx-drc1 master instance succeeded, u1i1", result.getMessage());


            // change back for further use
            result = drcMaintenanceService.changeMasterDb("fat-fx-drc1", "10.2.72.230", 55111);
            Assert.assertEquals(0, result.getStatus().intValue());
            Assert.assertEquals(2, ((Integer) result.getData()).intValue());
            Assert.assertEquals("update fat-fx-drc1 master instance succeeded, u2i0", result.getMessage());
        }
        
    }

    @Test
    public void testUpdateMhaGroup() throws Exception {
        for (EstablishStatusEnum establishStatusEnum : EstablishStatusEnum.values()) {
            drcMaintenanceService.updateMhaGroup(MHA1OY, MHA1RB, establishStatusEnum);
            MhaGroupTbl mhaGroupTbl = dalUtils.getMhaGroupTblDao().queryByPk(1L);
            Assert.assertEquals(establishStatusEnum.getCode(), (int) mhaGroupTbl.getDrcEstablishStatus());
        }

        // change back for further use
        drcMaintenanceService.updateMhaGroup(MHA1OY, MHA1RB, EstablishStatusEnum.ESTABLISHED);
        MhaGroupTbl mhaGroupTbl = dalUtils.getMhaGroupTblDao().queryByPk(1L);
        Assert.assertEquals((int) mhaGroupTbl.getDrcEstablishStatus(), EstablishStatusEnum.ESTABLISHED.getCode());
    }

    @Test
    public void testChangeMhaGroupStatus() throws Exception {

        MhaGroupPair mhaGroupPair = new MhaGroupPair();
        mhaGroupPair.setSrcMha(MHA1OY);
        mhaGroupPair.setDestMha(MHA1RB);

        drcMaintenanceService.changeMhaGroupStatus(mhaGroupPair, 55);
        MhaGroupTbl mhaGroupTbl = dalUtils.getMhaGroupTblDao().queryByPk(1L);
        Assert.assertEquals(55, (int) mhaGroupTbl.getDrcEstablishStatus());

        // change back for further use
        drcMaintenanceService.changeMhaGroupStatus(mhaGroupPair, EstablishStatusEnum.ESTABLISHED.getCode());
        mhaGroupTbl = dalUtils.getMhaGroupTblDao().queryByPk(1L);
        Assert.assertEquals((int) mhaGroupTbl.getDrcEstablishStatus(), EstablishStatusEnum.ESTABLISHED.getCode());
    }

    @Test
    public void testUpdateMhaDnsStatus() throws Throwable {

        drcMaintenanceService.updateMhaDnsStatus("nosuchmha", BooleanEnum.TRUE);
        List<MhaTbl> mhaTbls = dalUtils.getMhaTblDao().queryAll().stream().filter(m -> m.getDnsStatus().equals(BooleanEnum.TRUE.getCode())).collect(Collectors.toList());
        Assert.assertEquals(0, mhaTbls.size());

        drcMaintenanceService.updateMhaDnsStatus(MHA1OY, BooleanEnum.TRUE);
        mhaTbls = dalUtils.getMhaTblDao().queryAll().stream().filter(m -> m.getDnsStatus().equals(BooleanEnum.TRUE.getCode())).collect(Collectors.toList());
        Assert.assertEquals(1, mhaTbls.size());
        Assert.assertTrue(mhaTbls.get(0).getMhaName().equalsIgnoreCase(MHA1OY));

        // change back for further use
        drcMaintenanceService.updateMhaDnsStatus(MHA1OY, BooleanEnum.FALSE);
        mhaTbls = dalUtils.getMhaTblDao().queryAll().stream().filter(m -> m.getDnsStatus().equals(BooleanEnum.TRUE.getCode())).collect(Collectors.toList());
        Assert.assertEquals(0, mhaTbls.size());
    }


    @Test
    public void testUpdateMhaInstance() throws Throwable {
        MhaInstanceGroupDto dto = new MhaInstanceGroupDto();
        dto.setMhaName("no such mha");
        Assert.assertFalse(drcMaintenanceService.updateMhaInstances(dto));

        dto.setMhaName("fat-fx-drc1");
        dto.setMaster(new MhaInstanceGroupDto.MySQLInstance().setIp("127.0.0.1").setPort(4406).setIdc("SHAOY"));
        Assert.assertFalse(drcMaintenanceService.updateMhaInstances(dto));

        dto.setMaster(new MhaInstanceGroupDto.MySQLInstance().setIp("10.2.72.230").setPort(55111).setIdc("SHAOY"));
        dto.setSlaves(new ArrayList<>() {{
            add(new MhaInstanceGroupDto.MySQLInstance().setIp("10.2.72.247").setPort(55111).setIdc("SHAOY"));
        }});
        try(MockedStatic<MySqlUtils> theMock = Mockito.mockStatic(MySqlUtils.class)) {
            theMock.when(()-> MySqlUtils.getUuid(Mockito.anyString(),Mockito.anyInt(),Mockito.anyString(),Mockito.anyString(),Mockito.anyBoolean())).thenReturn("uuid");
            Assert.assertTrue(drcMaintenanceService.updateMhaInstances(dto));
        }
    }

    @Test
    public void testInputResource() throws Exception {
        Long id = dalUtils.getId(TableEnum.RESOURCE_TABLE, "127.0.0.1");
        Assert.assertNull(id);
        boolean result = drcMaintenanceService.inputResource("127.0.0.1", "shaoy", "R");
        Assert.assertTrue(result);
        id = dalUtils.getId(TableEnum.RESOURCE_TABLE, "127.0.0.1");
        Assert.assertNotNull(id);

        // change back for further use
        int rowsAffected = drcMaintenanceService.deleteResource("127.0.0.1");
        Assert.assertEquals(1, rowsAffected);
        rowsAffected = drcMaintenanceService.deleteResource("127.0.0.1");
        Assert.assertEquals(0, rowsAffected);
    }

    @Test
    public void testDeleteMachine() throws Exception {
        String ip = "10.2.3.4";
        dalUtils.updateOrCreateMachine(ip, 1234, "uuid", BooleanEnum.TRUE, 1L);
        Assert.assertEquals(0, drcMaintenanceService.deleteMachine(ip));
        dalUtils.updateOrCreateMachine(ip, 1234, "uuid", BooleanEnum.FALSE, 1L);
        Assert.assertEquals(1, drcMaintenanceService.deleteMachine(ip));
        Assert.assertEquals(0, drcMaintenanceService.deleteMachine(ip));
    }

    @Test
    public void testUpdateMasterReplicator() {
        Map<String, ReplicatorTbl> replicators = metaInfoServiceImpl.getReplicators("fat-fx-drc1");
        Mockito.doReturn(replicators).when(metaInfoService).getReplicators("fat-fx-drc1");
        Assert.assertTrue(drcMaintenanceService.updateMasterReplicator("fat-fx-drc1", "10.2.83.105"));
        replicators = metaInfoServiceImpl.getReplicators("fat-fx-drc1");
        Mockito.doReturn(replicators).when(metaInfoService).getReplicators("fat-fx-drc1");
        Assert.assertTrue(drcMaintenanceService.updateMasterReplicator("fat-fx-drc1", "10.2.87.154"));
        replicators = metaInfoServiceImpl.getReplicators("fat-fx-drc1");
        Mockito.doReturn(replicators).when(metaInfoService).getReplicators("fat-fx-drc1");
        Assert.assertFalse(drcMaintenanceService.updateMasterReplicator("fat-fx-drc1", "10.2.83.111"));
    }

    @Test
    public void testDeleteRoute() throws SQLException {
        Long oyId = dalUtils.getDcTblDao().queryAll().stream().filter(p -> p.getDcName().equalsIgnoreCase("shaoy")).findFirst().get().getId();
        Long rbId = dalUtils.getDcTblDao().queryAll().stream().filter(p -> p.getDcName().equalsIgnoreCase("sharb")).findFirst().get().getId();
        Long buId = dalUtils.getId(TableEnum.BU_TABLE, "BBZ");

        List<ProxyTbl> proxyTbls = dalUtils.getProxyTblDao().queryAll();
        Long proxyTlsOy1Id = proxyTbls.stream().filter(p -> p.getUri().equalsIgnoreCase(PROXYTLS_DC1_1)).findFirst().get().getId();
        Long proxyOy1Id = proxyTbls.stream().filter(p -> p.getUri().equalsIgnoreCase(PROXY_DC1_1)).findFirst().get().getId();
        Long proxyTlsOy2Id = proxyTbls.stream().filter(p -> p.getUri().equalsIgnoreCase(PROXYTLS_DC1_2)).findFirst().get().getId();
        Long proxyOy2Id = proxyTbls.stream().filter(p -> p.getUri().equalsIgnoreCase(PROXY_DC1_2)).findFirst().get().getId();
        Long proxyTlsRb1Id = proxyTbls.stream().filter(p -> p.getUri().equalsIgnoreCase(PROXYTLS_DC2_1)).findFirst().get().getId();
        Long proxyRb1Id = proxyTbls.stream().filter(p -> p.getUri().equalsIgnoreCase(PROXY_DC2_1)).findFirst().get().getId();
        Long proxyTlsRb2Id = proxyTbls.stream().filter(p -> p.getUri().equalsIgnoreCase(PROXYTLS_DC2_2)).findFirst().get().getId();
        Long proxyRb2Id = proxyTbls.stream().filter(p -> p.getUri().equalsIgnoreCase(PROXY_DC2_2)).findFirst().get().getId();
        Long proxyRelayDcId = proxyTbls.stream().filter(p -> p.getUri().equalsIgnoreCase(PROXY_DC_RELAY)).findFirst().get().getId();
        // init check before delete
        List<RouteTbl> routeTbls = dalUtils.getRouteTblDao().queryAll().stream()
                .filter(p -> p.getDeleted().equals(BooleanEnum.FALSE.getCode()) &&
                        p.getRouteOrgId().equals(buId) &&
                        p.getSrcDcId().equals(rbId) &&
                        p.getDstDcId().equals(oyId) &&
                        p.getTag().equalsIgnoreCase("console"))
                .collect(Collectors.toList());
        Assert.assertEquals(1, routeTbls.size());
        RouteTbl routeTbl = routeTbls.get(0);
        Assert.assertNotNull(routeTbl);
        Assert.assertEquals(buId, routeTbl.getRouteOrgId());
        Assert.assertEquals(rbId, routeTbl.getSrcDcId());
        Assert.assertEquals(oyId, routeTbl.getDstDcId());
        Assert.assertTrue("console".equalsIgnoreCase(routeTbl.getTag()));
        Assert.assertEquals(String.format("%s", proxyRb1Id), routeTbl.getSrcProxyIds());
        Assert.assertEquals(String.format("%s", proxyTlsOy1Id), routeTbl.getDstProxyIds());

        ApiResult result = drcMaintenanceService.deleteRoute("BBZ", "sharb", "shaoy", "console");
        Assert.assertEquals(0, result.getStatus().intValue());
        // check after success
        routeTbl = dalUtils.getRouteTblDao().queryAll().stream()
                .filter(p -> p.getDeleted().equals(BooleanEnum.FALSE.getCode()) &&
                        p.getRouteOrgId().equals(buId) &&
                        p.getSrcDcId().equals(rbId) &&
                        p.getDstDcId().equals(oyId) &&
                        p.getTag().equalsIgnoreCase("console"))
                .findFirst().orElse(null);
        Assert.assertNull(routeTbl);
    }

    @Test
    public void testInputDc() throws SQLException {
        ApiResult result = drcMaintenanceService.inputDc("dc1");
        Assert.assertEquals(ResultCode.HANDLE_SUCCESS.getCode(), result.getStatus().intValue());
        Long id = dalUtils.getId(TableEnum.DC_TABLE, "dc1");
        Assert.assertNotNull(id);
        id = dalUtils.getId(TableEnum.DC_TABLE, "dc2");
        Assert.assertNull(id);
    }

    @Test
    public void testInputAndDeleteGroupMapping() throws SQLException {
        drcMaintenanceService.inputGroupMapping(100L, 1000L);
        GroupMappingTbl groupMappingTbl = dalUtils.getGroupMappingTblDao().queryAll().stream()
                .filter(p -> BooleanEnum.FALSE.getCode().equals(p.getDeleted()) && p.getMhaGroupId().equals(100L) && p.getMhaId().equals(1000L))
                .findFirst().orElse(null);
        Assert.assertNotNull(groupMappingTbl);

        drcMaintenanceService.deleteGroupMapping(100L, 1000L);
        groupMappingTbl = dalUtils.getGroupMappingTblDao().queryAll().stream()
                .filter(p -> BooleanEnum.FALSE.getCode().equals(p.getDeleted()) && p.getMhaGroupId().equals(100L) && p.getMhaId().equals(1000L))
                .findFirst().orElse(null);
        Assert.assertNull(groupMappingTbl);
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
        
        List<MhaInstanceGroupDto.MySQLInstance> slaves = Lists.newArrayList();
        slaves.add(slave1);
        slaves.add(slave2);
        dto.setSlaves(slaves);

        //init tbl
        MhaGroupTbl mhaGroupTbl = new MhaGroupTbl();
        mhaGroupTbl.setMonitorUser("usr");
        mhaGroupTbl.setMonitorPassword("psw");
        Mockito.when(metaInfoService.getMhaGroupForMha("mhaName")).thenReturn(mhaGroupTbl);

        MhaTbl mhaTbl = new MhaTbl();
        mhaTbl.setId(1L);
        mhaTbl.setMhaName("mhaName");
        
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
            drcMaintenanceService.checkChange(
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
            drcMaintenanceService.checkChange(
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
            drcMaintenanceService.checkChange(
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
            drcMaintenanceService.checkChange(
                    dto,machinesInMetaDb,mhaTbl,insertMachines,updateMachines,deleteMachines
            );
            Assert.assertEquals(1,insertMachines.size());
            Assert.assertEquals(2,updateMachines.size());
            Assert.assertEquals(1,deleteMachines.size());
            
            drcMaintenanceService.mhaInstancesChange(dto,mhaTbl);
        }
        
        //test validate 
        try {
            dto.setMaster(null);
            dto.transferToMachine();
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
        }


    }
    
   
    
}
