package com.ctrip.framework.drc.console.service.v2;

import com.alibaba.fastjson.JSON;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.BuTblDao;
import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.ProxyTblDao;
import com.ctrip.framework.drc.console.dao.RouteTblDao;
import com.ctrip.framework.drc.console.dao.entity.BuTbl;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.ProxyTbl;
import com.ctrip.framework.drc.console.dao.entity.RouteTbl;
import com.ctrip.framework.drc.console.dto.RouteDto;
import com.ctrip.framework.drc.console.service.v2.resource.impl.RouteServiceImpl;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static com.ctrip.framework.drc.console.service.v2.MetaGeneratorBuilder.*;
import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.getBuTbl;
import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.getDcTbls;

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
    private RouteTblDao routeTblDao;
    @Mock
    private ProxyTblDao proxyTblDao;
    @Mock
    private DefaultConsoleConfig consoleConfig;

    public static String PROXY = "PROXY";
    public static String IP_DC1_1 = "10.25.222.15";
    public static String PORT_IN = "80";

    public static String PROXY_DC1_1 = String.format("%s://%s:%s", PROXY, IP_DC1_1, PORT_IN);

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testSubmitProxyRouteConfig() throws Exception {
        RouteDto routeDto = new RouteDto();
        routeDto.setId(0L);
        routeDto.setRouteOrgName("BBZ");
        routeDto.setSrcDcName("shaoy");
        routeDto.setDstDcName("sharb");
        routeDto.setSrcProxyUris(Arrays.asList("URI"));
        routeDto.setRelayProxyUris(Arrays.asList("URI"));
        routeDto.setDstProxyUris(Arrays.asList("URI"));
        routeDto.setTag("meta");

        Mockito.when(buTblDao.queryByBuName(Mockito.anyString())).thenReturn(getBuTbl());
        Mockito.when(dcTblDao.queryByDcName(Mockito.anyString())).thenReturn(getDcTbls().get(0));
        Mockito.when(proxyTblDao.queryByUri(Mockito.anyString())).thenReturn(getProxyTbls().get(0));
        Mockito.doNothing().when(routeTblDao).upsert(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong(),
                Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyInt());

        String result = routeService.submitProxyRouteConfig(routeDto);
        Assert.assertEquals(result, "update proxy route succeeded");
    }

    @Test
    public void testGetProxyRoutes() throws Exception {
        List<BuTbl> buTbls = this.getData("BuTbl.json", BuTbl.class);
        List<RouteTbl> routeTblList = this.getData("RouteTbl.json", RouteTbl.class);
        List<ProxyTbl> proxyTblList = this.getData("ProxyTbl.json", ProxyTbl.class);

        Mockito.when(proxyTblDao.queryAllExist()).thenReturn(proxyTblList);
        Mockito.when(routeTblDao.queryAll()).thenReturn(routeTblList);
        Mockito.when(buTblDao.queryByBuName(Mockito.anyString())).thenReturn(buTbls.get(0));
        Mockito.when(buTblDao.queryByPk(Mockito.anyLong())).thenReturn(buTbls.get(0));

        List<DcTbl> dcTbls = this.getData("DcTbl.json", DcTbl.class);
        Mockito.when(dcTblDao.queryByDcName(Mockito.eq("shaoy"))).thenReturn(dcTbls.get(0));
        Mockito.when(dcTblDao.queryByDcName(Mockito.eq("sinaws"))).thenReturn(dcTbls.get(1));
        Mockito.when(dcTblDao.queryByPk(Mockito.anyLong())).thenReturn(dcTbls.get(0));

        List<RouteDto> proxyRoutes = routeService.getRoutes("BBZ", "shaoy", "sinaws", "meta",0);
        Assert.assertEquals(1, proxyRoutes.size());
        for (RouteDto dto : proxyRoutes) {
            Assert.assertEquals("shaoy", dto.getSrcDcName());
            Assert.assertEquals("shaoy", dto.getDstDcName());
            Assert.assertEquals("BBZ", dto.getRouteOrgName());
            Assert.assertTrue("meta".equalsIgnoreCase(dto.getTag()) || "console".equalsIgnoreCase(dto.getTag()));
            List<String> srcProxyIps = dto.getSrcProxyUris();
            List<String> dstProxyIps = dto.getDstProxyUris();
            Assert.assertEquals(1, srcProxyIps.size());
            Assert.assertEquals(0, dstProxyIps.size());
            Assert.assertTrue(srcProxyIps.contains(PROXY_DC1_1));
        }
    }

    @Test
    public void testDeleteRoute() throws SQLException {
        Mockito.when(buTblDao.queryByBuName(Mockito.anyString())).thenReturn(getButbls().get(0));
        Mockito.when(dcTblDao.queryByDcName(Mockito.anyString())).thenReturn(MetaGeneratorBuilder.getDcTbls().get(0));
        Mockito.when(routeTblDao.queryAllExist()).thenReturn(getRouteTbls());
        Mockito.when(routeTblDao.update(Mockito.any(RouteTbl.class))).thenReturn(1);

        routeService.deleteRoute("route", "srcDc", "dstDc", "console");
        Mockito.verify(routeTblDao, Mockito.times(1)).update(Mockito.any(RouteTbl.class));
    }

    public  <T> List<T> getData(String fileName, Class<T> clazz) {
        String prefix = "/testData/messengerServiceV2/";
        String pathPrefix = System.getProperty("mock.data.path.prefix");
        if (StringUtils.isNotBlank(pathPrefix) ) {
            prefix = pathPrefix;
        }
        try {
            String json = IOUtils.toString(Objects.requireNonNull(this.getClass().getResourceAsStream(prefix + fileName)), StandardCharsets.UTF_8);
            return JSON.parseArray(json, clazz);
        } catch (Exception e) {
            System.out.println("empty file" + prefix + fileName);
            return Collections.emptyList();
        }
    }
}
