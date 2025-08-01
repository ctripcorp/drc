package com.ctrip.framework.drc.console.monitor.delay.server;

import com.ctrip.framework.drc.console.dao.entity.BuTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.monitor.delay.config.DelayMonitorSlaveConfig;
import com.ctrip.framework.drc.console.service.v2.CentralService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Created by shiruixin
 * 2025/7/30 15:36
 */
public class StaticDelayMonitorServerTest3 {
    private CentralService centralService = Mockito.mock(CentralService.class);
    private StaticDelayMonitorServer server;
    private DelayMonitorSlaveConfig mySQLSlaveConfig = Mockito.mock(DelayMonitorSlaveConfig.class);

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        when(mySQLSlaveConfig.getRegistryKey()).thenReturn("r");
        server = new StaticDelayMonitorServer(mySQLSlaveConfig, null,
                null, null, centralService, 0);
    }

    @Test
    public void testGetBuNameByMhaName() throws SQLException {
        when(centralService.getAllBuTbls()).thenReturn(new ArrayList<BuTbl>() {{
            BuTbl b1 = new BuTbl();
            b1.setId(10L);
            b1.setBuName("bu1");
            BuTbl b2 = new BuTbl();
            b2.setId(20L);
            b2.setBuName("bu2");
            add(b1);
            add(b2);
        }});
        when(centralService.queryAllMhaTblV2()).thenReturn(new ArrayList<MhaTblV2>() {{
            MhaTblV2 m1 = new MhaTblV2();
            m1.setMhaName("mha10");
            m1.setBuId(10L);
            add(m1);
        }});
        String bu = server.getBuNameByMhaName("mha10");
        assertEquals("bu1", bu);
    }

    @Test
    public void testGetBuNameByMhaName2() throws SQLException {
        when(centralService.getAllBuTbls()).thenReturn(new ArrayList<BuTbl>() {{
            BuTbl b1 = new BuTbl();
            b1.setId(1L);
            b1.setBuName("bu1");
            BuTbl b2 = new BuTbl();
            b2.setId(2L);
            b2.setBuName("bu2");
            add(b1);
            add(b2);
        }});
        when(centralService.queryAllMhaTblV2()).thenReturn(new ArrayList<MhaTblV2>() {{
            MhaTblV2 m1 = new MhaTblV2();
            m1.setMhaName("mha1");
            m1.setBuId(1L);
            add(m1);
        }});
        String bu = server.getBuNameByMhaName("mha2");
        assertEquals("BU", bu);

        when(centralService.getAllBuTbls()).thenReturn(new ArrayList<BuTbl>() {{
            BuTbl b1 = new BuTbl();
            b1.setId(1L);
            b1.setBuName("bu1");
            BuTbl b2 = new BuTbl();
            b2.setId(2L);
            b2.setBuName("bu2");
            add(b1);
            add(b2);
        }});
        when(centralService.queryAllMhaTblV2()).thenReturn(new ArrayList<MhaTblV2>() {{
            MhaTblV2 m1 = new MhaTblV2();
            m1.setMhaName("mha3");
            m1.setBuId(3L);
            add(m1);
        }});
        bu = server.getBuNameByMhaName("mha3");
        assertEquals("BU", bu);
    }

    @Test
    public void testGetBuNameByMhaName3() throws SQLException {
        when(centralService.getAllBuTbls()).thenReturn(new ArrayList<BuTbl>() {{
            BuTbl b1 = new BuTbl();
            b1.setId(1L);
            b1.setBuName("bu1");
            BuTbl b2 = new BuTbl();
            b2.setId(2L);
            b2.setBuName("bu2");
            add(b1);
            add(b2);
        }});
        when(centralService.queryAllMhaTblV2()).thenReturn(null);
        String bu = server.getBuNameByMhaName("mha1");
        assertEquals("BU", bu);

        when(centralService.getAllBuTbls()).thenReturn(null);
        when(centralService.queryAllMhaTblV2()).thenReturn(new ArrayList<MhaTblV2>() {{
            MhaTblV2 m1 = new MhaTblV2();
            m1.setMhaName("mha1");
            m1.setBuId(3L);
            add(m1);
        }});
        bu = server.getBuNameByMhaName("mha1");
        assertEquals("BU", bu);
    }
}
