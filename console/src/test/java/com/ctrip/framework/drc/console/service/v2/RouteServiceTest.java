package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.dto.RouteDto;
import com.ctrip.framework.drc.console.dto.RouteMappingDto;
import com.ctrip.framework.drc.console.dto.v3.MhaDbDto;
import com.ctrip.framework.drc.console.dto.v3.MhaDbReplicationDto;
import com.ctrip.framework.drc.console.param.MhaDbReplicationRouteDto;
import com.ctrip.framework.drc.console.param.MhaRouteMappingDto;
import com.ctrip.framework.drc.console.param.RouteQueryParam;
import com.ctrip.framework.drc.console.service.v2.resource.impl.RouteServiceImpl;
import com.ctrip.framework.drc.console.vo.v2.ApplierReplicationView;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

/**
 * Created by dengquanliang
 * 2023/12/6 20:30
 */
public class RouteServiceTest {

    @InjectMocks
    private RouteServiceImpl routeService;

    @Mock
    private BuTblDao buTblDao;
    @Mock
    private DcTblDao dcTblDao;
    @Mock
    private ProxyTblDao proxyTblDao;
    @Mock
    private RouteTblDao routeTblDao;
    @Mock
    private DefaultConsoleConfig consoleConfig;
    @Mock
    private MhaDbReplicationService mhaDbReplicationService;
    @Mock
    private DbReplicationRouteMappingTblDao routeMappingTblDao;
    @Mock
    private MhaTblV2Dao mhaTblV2Dao;


    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        Mockito.when(buTblDao.queryByBuName(Mockito.anyString())).thenReturn(getBuTbls().get(0));
        Mockito.when(dcTblDao.queryByDcName(Mockito.anyString())).thenReturn(getDcTbls().get(0));

        Mockito.when(dcTblDao.queryAllExist()).thenReturn(getDcTbls());
        Mockito.when(buTblDao.queryAllExist()).thenReturn(getBuTbls());
        Mockito.when(proxyTblDao.queryAllExist()).thenReturn(getProxyTbls());

        Mockito.when(routeTblDao.queryById(Mockito.eq(1L))).thenReturn(getRouteTbls().get(0));
        Mockito.when(routeTblDao.queryById(Mockito.eq(2L))).thenReturn(getRouteTbls().get(1));
        Mockito.when(routeTblDao.queryById(Mockito.eq(3L))).thenReturn(getRouteTbls().get(2));
        Mockito.when(routeTblDao.update(Mockito.any(RouteTbl.class))).thenReturn(1);

        Mockito.when(routeMappingTblDao.queryByRouteId(Mockito.anyLong())).thenReturn(getRouteMappingTbls());
        Mockito.when(routeMappingTblDao.update(Mockito.anyList())).thenReturn(new int[0]);


    }


    @Test
    public void testSubmitRoute() throws Exception {
        Mockito.when(routeTblDao.queryById(Mockito.anyLong())).thenReturn(getRouteTbls().get(0));
        Mockito.when(routeTblDao.insert(Mockito.any(RouteTbl.class))).thenReturn(1);

        RouteDto routeDto = new RouteDto();
        routeDto.setRouteOrgName("BU");
        routeDto.setSrcDcName("dc1");
        routeDto.setDstDcName("dc1");
        routeDto.setSrcProxyUris(Lists.newArrayList("proxy"));
        routeDto.setDstProxyUris(Lists.newArrayList("proxy"));
        routeDto.setRelayProxyUris(Lists.newArrayList("proxy"));
        routeDto.setTag("tag");

        routeService.submitRoute(routeDto);
        Mockito.verify(routeTblDao, times(1)).insert(Mockito.any(RouteTbl.class));

        routeDto.setId(1L);
        routeService.submitRoute(routeDto);
        Mockito.verify(routeTblDao, times(1)).update(Mockito.any(RouteTbl.class));
    }

    @Test
    public void testGetRoutes() throws Exception {
        Mockito.when(routeTblDao.queryByParam(Mockito.any())).thenReturn(Lists.newArrayList(getRouteTbls()));
        RouteQueryParam param = new RouteQueryParam();
        param.setSrcDcName("dc");
        param.setDstDcName("dc");

        List<RouteDto> routes = routeService.getRoutes(param);
        Assert.assertEquals(getRouteTbls().size(), routes.size());
    }

    @Test
    public void testGetRoute() throws Exception {
        RouteDto route = routeService.getRoute(1L);
        Assert.assertEquals("BU", route.getRouteOrgName());
    }

    @Test
    public void testDeleteRoute() throws Exception {
        routeService.deleteRoute(1L);
        Mockito.verify(routeMappingTblDao, times(1)).update(Mockito.anyList());
    }

    @Test
    public void testActiveRoute() throws Exception {
        Mockito.when(routeTblDao.queryById(Mockito.eq(1L))).thenReturn(getRouteTbls().get(0));
        routeService.activeRoute(1L);
        Mockito.verify(routeMappingTblDao, times(1)).update(Mockito.anyList());
    }

    @Test
    public void testDeactivateRoute() throws Exception {
        routeService.deactivateRoute(2L);
        Mockito.verify(routeTblDao, times(1)).update(Mockito.any(RouteTbl.class));
    }

    @Test
    public void testSubmitMhaDbReplicationRoutes() throws Exception {
        Mockito.when(routeMappingTblDao.queryByMhaDbReplicationIds(Mockito.anyList())).thenReturn(getRouteMappingTbls());

        Mockito.when(routeMappingTblDao.update(Mockito.anyList())).thenReturn(new int[0]);
        Mockito.when(routeMappingTblDao.insert(Mockito.anyList())).thenReturn(new int[0]);

        MhaDbReplicationRouteDto routeDto = new MhaDbReplicationRouteDto();
        routeDto.setRouteId(1L);
        routeDto.setMhaDbReplicationIds(Lists.newArrayList(1L));
        routeService.submitMhaDbReplicationRoutes(routeDto);
        Mockito.verify(routeMappingTblDao, never()).update(Mockito.anyList());
        Mockito.verify(routeMappingTblDao, never()).insert(Mockito.anyList());

        routeDto.setRouteId(3L);
        routeService.submitMhaDbReplicationRoutes(routeDto);
        Mockito.verify(routeMappingTblDao, times(1)).update(Mockito.anyList());
        Mockito.verify(routeMappingTblDao, never()).insert(Mockito.anyList());

        routeDto.setRouteId(1L);
        routeDto.setMhaDbReplicationIds(Lists.newArrayList(2L, 3L));
        routeService.submitMhaDbReplicationRoutes(routeDto);
        Mockito.verify(routeMappingTblDao, times(2)).update(Mockito.anyList());
        Mockito.verify(routeMappingTblDao, times(1)).insert(Mockito.anyList());
    }

    @Test
    public void testSubmitMhaRoutes() throws Exception {
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.anyString(), Mockito.anyInt())).thenReturn(getMhaTblV2s().get(0));
        Mockito.when(routeMappingTblDao.queryByMhaId(Mockito.anyLong())).thenReturn(getRouteMappingTbls());

        MhaRouteMappingDto routeMappingDto = new MhaRouteMappingDto();
        routeMappingDto.setMhaName("mha");
        routeMappingDto.setRouteIds(Lists.newArrayList(1L));
        routeService.submitMhaRoutes(routeMappingDto);
        Mockito.verify(routeMappingTblDao, never()).insert(Mockito.anyList());


        Mockito.when(routeMappingTblDao.queryByMhaId(Mockito.anyLong())).thenReturn(new ArrayList<>());
        routeMappingDto.setRouteIds(Lists.newArrayList(1L));
        routeService.submitMhaRoutes(routeMappingDto);
        Mockito.verify(routeMappingTblDao, times(1)).insert(Mockito.anyList());
    }

    @Test
    public void testDeleteMhaDbReplicationRoutes() throws Exception {
        MhaDbReplicationRouteDto routeDto = new MhaDbReplicationRouteDto();
        routeDto.setRouteId(1L);
        routeDto.setMhaDbReplicationIds(Lists.newArrayList(1L));

        Mockito.when(routeMappingTblDao.queryByMhaDbReplicationIds(Mockito.anyLong(), Mockito.anyList())).thenReturn(new ArrayList<>());
        routeService.deleteMhaDbReplicationRoutes(routeDto);
        Mockito.verify(routeMappingTblDao, never()).update(Mockito.anyList());

        Mockito.when(routeMappingTblDao.queryByMhaDbReplicationIds(Mockito.anyLong(), Mockito.anyList())).thenReturn(getRouteMappingTbls());
        routeService.deleteMhaDbReplicationRoutes(routeDto);
        Mockito.verify(routeMappingTblDao, times(1)).update(Mockito.anyList());
    }

    @Test
    public void testDeleteMhaRoutes() throws Exception {
        MhaDbReplicationRouteDto routeDto = new MhaDbReplicationRouteDto();
        routeDto.setRouteId(1L);
        routeDto.setMhaIds(Lists.newArrayList(1L));

        Mockito.when(routeMappingTblDao.queryByMhaIds(Mockito.anyLong(), Mockito.anyList())).thenReturn(new ArrayList<>());
        routeService.deleteMhaRoutes(routeDto);
        Mockito.verify(routeMappingTblDao, never()).update(Mockito.anyList());

        Mockito.when(routeMappingTblDao.queryByMhaIds(Mockito.anyLong(), Mockito.anyList())).thenReturn(getRouteMappingTbls());
        routeService.deleteMhaRoutes(routeDto);
        Mockito.verify(routeMappingTblDao, times(1)).update(Mockito.anyList());
    }

    @Test
    public void testGetMhaRouteMappings() throws Exception {
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.anyString(), Mockito.anyInt())).thenReturn(getMhaTblV2s().get(0));
        Mockito.when(routeMappingTblDao.queryByMhaId(Mockito.anyLong())).thenReturn(getRouteMappingTbls());
        Mockito.when(routeTblDao.queryAllExist()).thenReturn(getRouteTbls());

        List<RouteMappingDto> res = routeService.getRouteMappings("mha");
        Assert.assertEquals(getRouteMappingTbls().size(), res.size());
    }


    @Test
    public void testGetRoutesByRegion() throws Exception {
        Mockito.when(routeTblDao.queryByParam(Mockito.any())).thenReturn(Lists.newArrayList(getRouteTbls()));
        List<RouteDto> res = routeService.getRoutesByRegion("dc", "dc");
        Assert.assertEquals(getRouteTbls().size(), res.size());
    }

    @Test
    public void testGetRelatedMhas() throws Exception {
        Mockito.when(routeMappingTblDao.queryByRouteId(Mockito.anyLong())).thenReturn(getRouteMappingTbls());
        Mockito.when(mhaTblV2Dao.queryByIds(Mockito.anyList())).thenReturn(getMhaTblV2s());

        List<ApplierReplicationView> res = routeService.getRelatedMhas(1L);
        Assert.assertEquals(getMhaTblV2s().size(), res.size());
    }

    @Test
    public void testGetRoutesByDstRegion() throws Exception {
        Mockito.when(routeTblDao.queryByParam(Mockito.any())).thenReturn(Lists.newArrayList(getRouteTbls()));
        List<RouteDto> res = routeService.getRoutesByDstRegion("dc");
        Assert.assertEquals(getRouteTbls().size(), res.size());
    }

    @Test
    public void testGetRouteMappings() throws Exception {
        Mockito.when(mhaDbReplicationService.queryByMha(Mockito.anyString(), Mockito.anyString(), Mockito.any())).thenReturn(getReplications());
        Mockito.when(routeMappingTblDao.queryByMhaDbReplicationIds(Mockito.anyList())).thenReturn(getRouteMappingTbls());
        Mockito.when(routeTblDao.queryAllExist()).thenReturn(getRouteTbls());
        List<RouteMappingDto> res = routeService.getRouteMappings("srcMha", "dstMha");
        Assert.assertEquals(1, res.size());
    }

    @Test
    public void testGetRelatedDbs() throws Exception {
        Mockito.when(routeMappingTblDao.queryByRouteId(Mockito.anyLong())).thenReturn(getRouteMappingTbls());

        List<ApplierReplicationView> views = new ArrayList<>();
        Mockito.when(mhaDbReplicationService.query(Mockito.anyList())).thenReturn(views);
        List<ApplierReplicationView> res = routeService.getRelatedDbs(1L);
        Assert.assertEquals(0, res.size());
    }

    private List<MhaDbReplicationDto> getReplications() {
        MhaDbReplicationDto replication = new MhaDbReplicationDto();
        replication.setId(1L);
        MhaDbDto src = new MhaDbDto(1L, "srcMha", "db");
        MhaDbDto dst = new MhaDbDto(2L, "dstMha", "db");
        replication.setSrc(src);
        replication.setDst(dst);
        return Lists.newArrayList(replication);
    }

    private List<DbReplicationRouteMappingTbl> getRouteMappingTbls() {
        DbReplicationRouteMappingTbl routeMappingTbl = new DbReplicationRouteMappingTbl();
        routeMappingTbl.setId(1L);
        routeMappingTbl.setRouteId(1L);
        routeMappingTbl.setMhaDbReplicationId(1L);
        routeMappingTbl.setMhaId(1L);

        return Lists.newArrayList(routeMappingTbl);
    }

    public static List<BuTbl> getBuTbls() {
        BuTbl buTbl = new BuTbl();
        buTbl.setBuName("BU");
        buTbl.setId(1L);
        return Lists.newArrayList(buTbl);
    }

    public static List<RouteTbl> getRouteTbls() {
        RouteTbl routeTbl = new RouteTbl();
        routeTbl.setId(1L);
        routeTbl.setRouteOrgId(1L);
        routeTbl.setSrcProxyIds("1");
        routeTbl.setDstProxyIds("1");
        routeTbl.setGlobalActive(0);

        RouteTbl routeTbl1 = new RouteTbl();
        routeTbl1.setId(2L);
        routeTbl1.setRouteOrgId(1L);
        routeTbl1.setSrcProxyIds("1");
        routeTbl1.setDstProxyIds("1");
        routeTbl1.setGlobalActive(1);

        RouteTbl routeTbl2 = new RouteTbl();
        routeTbl2.setId(3L);
        routeTbl2.setRouteOrgId(1L);
        routeTbl2.setSrcProxyIds("1");
        routeTbl2.setDstProxyIds("1");
        routeTbl2.setGlobalActive(0);

        return Lists.newArrayList(routeTbl, routeTbl1, routeTbl2);
    }

    public static List<ProxyTbl> getProxyTbls() {
        ProxyTbl proxyTbl = new ProxyTbl();
        proxyTbl.setId(1L);
        proxyTbl.setUri("proxy");
        proxyTbl.setDcId(1L);

        return Lists.newArrayList(proxyTbl);
    }

    public static List<DcTbl> getDcTbls() {
        DcTbl dcTbl = new DcTbl();
        dcTbl.setDcName("dc");
        dcTbl.setId(1L);
        dcTbl.setRegionName("region");

        return Lists.newArrayList(dcTbl);
    }

    private List<MhaTblV2> getMhaTblV2s() {
        MhaTblV2 mhatblV2 = new MhaTblV2();
        mhatblV2.setId(1L);
        mhatblV2.setMhaName("mha");

        return Lists.newArrayList(mhatblV2);
    }

}
