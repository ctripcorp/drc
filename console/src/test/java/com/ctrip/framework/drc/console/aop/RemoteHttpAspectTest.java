package com.ctrip.framework.drc.console.aop;

import com.ctrip.framework.drc.console.aop.forward.RemoteHttpAspect;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.service.v2.CacheMetaService;
import com.ctrip.framework.drc.console.service.v2.CentralService;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.service.v2.impl.CentralServiceImpl;
import com.ctrip.framework.drc.console.service.v2.impl.MysqlServiceV2Impl;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.console.service.v2.resource.impl.ResourceServiceImpl;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class RemoteHttpAspectTest {

    @InjectMocks
    private RemoteHttpAspect aop;

    @Mock
    private DefaultConsoleConfig consoleConfig;

    @Mock
    private CentralServiceImpl centralService;

    @Mock
    private CacheMetaService cacheMetaService;

    @Spy
    private MysqlServiceV2Impl mysqlServiceV2;

    private MysqlServiceV2 proxy;

    @Spy
    private ResourceServiceImpl resourceServiceSpy;

    private ResourceService proxy2;

    @Spy
    private CentralServiceImpl centralServiceSpy;
    private CentralService proxy3;


    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        Mockito.when(consoleConfig.getCenterRegion()).thenReturn("sha");
        Mockito.when(consoleConfig.getRegion()).thenReturn("region1");
        Mockito.when(consoleConfig.getCenterRegionUrl()).thenReturn("centerRegionUrl");
        Mockito.when(consoleConfig.getRegionForDc(Mockito.anyString())).thenReturn("region1");
        Mockito.when(consoleConfig.getPublicCloudRegion()).thenReturn(
                new HashSet<>() {{
                    add("region2");
                }}
        );
        Mockito.when(consoleConfig.getConsoleRegionUrls()).thenReturn(
                new HashMap<>() {{
                    put("region1", "url1");
                    put("region2", "url2");
                }}
        );
        AspectJProxyFactory factory = new AspectJProxyFactory(mysqlServiceV2);
        factory.setProxyTargetClass(true);
        factory.addAspect(aop);
        proxy = factory.getProxy();

        AspectJProxyFactory factory2 = new AspectJProxyFactory(resourceServiceSpy);
        factory2.setProxyTargetClass(true);
        factory2.addAspect(aop);
        proxy2 = factory2.getProxy();

        AspectJProxyFactory factory3 = new AspectJProxyFactory(centralServiceSpy);
        factory3.setProxyTargetClass(true);
        factory3.addAspect(aop);
        proxy3 = factory3.getProxy();

    }


    @Test
    public void testForwardToMetaDb() throws Exception {
        Mockito.when(consoleConfig.getRegion()).thenReturn("region1");
        proxy3.queryAllMhaTblV2();
        Mockito.verify(centralServiceSpy, Mockito.never()).queryAllMhaTblV2();

        Mockito.when(consoleConfig.getRegion()).thenReturn("sha");
        proxy3.queryAllMhaTblV2();
        Mockito.verify(centralServiceSpy, Mockito.times(1)).queryAllMhaTblV2();
    }

    @Test
    public void testForwardByArgsInResourceService() throws Exception {
        Map<String, Set<String>> map = Maps.newHashMap();
        map.put("sha", Sets.newHashSet("sharb"));
        map.put("sin", Sets.newHashSet("sinaws"));
        Mockito.when(consoleConfig.getRegion2dcsMapping()).thenReturn(map);
        Mockito.when(consoleConfig.getRegion()).thenReturn("sha");
        Mockito.when(consoleConfig.getPublicCloudRegion()).thenReturn(Sets.newHashSet("sin"));
        Map<String, String> map2 = Maps.newHashMap();
        map2.put("sha", "uri");
        map2.put("sin", "uri");
        Mockito.when(consoleConfig.getConsoleRegionUrls()).thenReturn(map2);
        try {
            proxy2.getMasterAppliersInRegion("sin", Lists.newArrayList());
        } catch (Exception e) {

        }
        Mockito.verify(resourceServiceSpy, Mockito.never()).getMasterAppliersInRegion(Mockito.anyString(), Mockito.anyList());

        try {
            proxy2.getMasterAppliersInRegion("sha", Lists.newArrayList());
        } catch (Exception e) {

        }
        Mockito.verify(resourceServiceSpy, Mockito.atLeastOnce()).getMasterAppliersInRegion(Mockito.anyString(), Mockito.anyList());
    }


    @Test
    public void testForwardByArgs() throws SQLException {

        Mockito.when(centralService.getDcName(Mockito.eq("mha1"))).thenReturn(getDc().getDcName());
        MySqlEndpoint mySqlEndpoint = new MySqlEndpoint("ip", 3306, "usr", "psw", true);
        Mockito.when(cacheMetaService.getMasterEndpoint(Mockito.anyString())).thenReturn(mySqlEndpoint);


        // forward
        Mockito.when(consoleConfig.getRegionForDc(Mockito.eq("dc2"))).thenReturn("region2");
        proxy.getMhaExecutedGtid("mha1");
        Mockito.verify(mysqlServiceV2, Mockito.never()).getMhaExecutedGtid(Mockito.anyString());

        // not forward
        // case1:localRegion is not a public cloud region
        Mockito.when(consoleConfig.getRegionForDc(Mockito.eq("dc2"))).thenReturn("region1");
        proxy.getMhaExecutedGtid("mha1");
        Mockito.verify(mysqlServiceV2, Mockito.atLeastOnce()).getMhaExecutedGtid(Mockito.anyString());

        // case2:localRegion is a public cloud region
        Mockito.when(consoleConfig.getRegion()).thenReturn("region2");
        proxy.getMhaExecutedGtid("mha1");

    }

    @Test
    public void testParseHttpArg() {
        Assert.assertEquals("ip1,ip2,ip333", RemoteHttpAspect.parseArgValue(Lists.newArrayList("ip1", "ip2","ip333")));
        Assert.assertEquals("testStr", RemoteHttpAspect.parseArgValue("testStr"));

    }

    private MhaTblV2 getMhaTblV2() {
        MhaTblV2 mhaTblV2 = new MhaTblV2();
        mhaTblV2.setDcId(1L);
        return mhaTblV2;
    }

    private DcTbl getDc() {
        DcTbl dcTbl = new DcTbl();
        dcTbl.setDcName("dc2");
        return dcTbl;
    }

}