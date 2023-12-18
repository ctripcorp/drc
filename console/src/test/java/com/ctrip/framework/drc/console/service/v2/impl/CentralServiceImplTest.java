package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.DdlHistoryTblDao;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.service.v2.MachineService;
import com.ctrip.framework.drc.console.service.v2.MhaDbReplicationService;
import java.sql.SQLException;
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
}