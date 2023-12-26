package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.MachineTblDao;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.Data;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.DbaClusterInfoResponse;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.MemberInfo;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.cache.LoadingCache;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

import java.sql.SQLException;
import java.util.List;

import static org.mockito.Mockito.*;

public class MachineServiceImplTest {
    @Mock
    Logger logger;
    @Mock
    LoadingCache<String, Endpoint> cache;
    @Mock
    MhaTblV2Dao mhaTblDao;
    @Mock
    MachineTblDao machineTblDao;
    @Mock
    DbaApiService dbaApiService;
    @Mock
    MonitorTableSourceProvider monitorTableSourceProvider;
    @InjectMocks
    MachineServiceImpl machineServiceImpl;

    @Before
    public void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        MhaTblV2 mhaTblV2 = new MhaTblV2();

        mhaTblV2.setId(1L);
        mhaTblV2.setMhaName("mha1");
        when(mhaTblDao.queryByMhaName(anyString(), anyInt())).thenReturn(mhaTblV2);
        when(monitorTableSourceProvider.getMonitorUserVal()).thenReturn("mockUser");
        when(monitorTableSourceProvider.getMonitorPasswordVal()).thenReturn("mockPassword");

        DbaClusterInfoResponse value = new DbaClusterInfoResponse();
        Data data = new Data();
        List<MemberInfo> memberInfos = Lists.newArrayList();
        MemberInfo memberInfo = new MemberInfo();
        memberInfo.setService_ip("ip1");
        memberInfo.setDns_port(3306);
        memberInfo.setRole("master");
        memberInfos.add(memberInfo);
        data.setMemberlist(memberInfos);
        value.setData(data);
        when(dbaApiService.getClusterMembersInfo(anyString())).thenReturn(value);
    }

    @Test
    public void testGetMasterEndpointCached() throws Exception {
        when(machineTblDao.queryByMhaId(anyLong(), anyInt())).thenReturn(null);
        for (int i = 0; i < 10; i++) {
            Endpoint result = machineServiceImpl.getMasterEndpointCached("mha1");
            Assert.assertEquals(new MySqlEndpoint("ip1", 3306, "mockUser", "mockPassword", true), result);
        }
        verify(dbaApiService, times(1)).getClusterMembersInfo("mha1");
    }

    @Test
    public void testGetMasterEndpoint() throws Exception {

        // case 1: get from dba api
        when(machineTblDao.queryByMhaId(anyLong(), anyInt())).thenReturn(null);
        Endpoint result = machineServiceImpl.getMasterEndpoint("mha1");
        Assert.assertEquals(new MySqlEndpoint("ip1", 3306, "mockUser", "mockPassword", true), result);

        // case 2: get from tbl
        when(machineTblDao.queryByMhaId(anyLong(), anyInt())).thenReturn(List.of(new MachineTbl("ip2", 3306, 1)));
        Endpoint result2 = machineServiceImpl.getMasterEndpoint("mha1");
        Assert.assertEquals(new MySqlEndpoint("ip2", 3306, "mockUser", "mockPassword", true), result2);

    }

    @Test
    public void testGetUuid() throws SQLException {
        MachineTbl machineTbl = new MachineTbl();
        machineTbl.setUuid("uuid1");
        when(machineTblDao.queryByIpPort(eq("ip1"), eq(3306))).thenReturn(machineTbl);
        Assert.assertEquals("uuid1",machineServiceImpl.getUuid("ip1", 3306));
        when(machineTblDao.queryByIpPort(eq("ip1"), eq(3306))).thenReturn(null);
        Assert.assertNull(machineServiceImpl.getUuid("ip1", 3306));
    }

    @Test
    public void testCorrectUuid() throws SQLException {
        when(machineTblDao.update(any(MachineTbl.class))).thenReturn(1);
        MachineTbl machineTbl = new MachineTbl();
        machineTbl.setUuid("uuid1");
        when(machineTblDao.queryByIpPort(anyString(), anyInt())).thenReturn(machineTbl);
        Assert.assertEquals(1,machineServiceImpl.correctUuid("ip1", 3306, "uuid2").intValue());
        when(machineTblDao.queryByIpPort(eq("ip1"), eq(3306))).thenReturn(null);
        Assert.assertEquals(0,machineServiceImpl.correctUuid("ip1", 3306, "uuid2").intValue());
    }
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme