package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.BuTblDao;
import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.ProxyTblDao;
import com.ctrip.framework.drc.console.service.v2.resource.impl.ProxyServiceImpl;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.console.service.v2.MetaGeneratorBuilder.getDcTbls;
import static com.ctrip.framework.drc.console.service.v2.MetaGeneratorBuilder.getProxyTbls;

/**
 * Created by dengquanliang
 * 2023/12/6 21:12
 */
public class ProxyServiceTest {

    @InjectMocks
    private ProxyServiceImpl proxyService;

    @Mock
    private DefaultConsoleConfig consoleConfig;
    @Mock
    private ProxyTblDao proxyTblDao;
    @Mock
    private DcTblDao dcTblDao;
    @Mock
    private BuTblDao buTblDao;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testGetProxyUris() throws Exception {
        Mockito.when(consoleConfig.getDcsInSameRegion(Mockito.anyString())).thenReturn(Sets.newHashSet("dc"));
        Mockito.when(dcTblDao.queryByDcName(Mockito.anyString())).thenReturn(getDcTbls().get(0));
        Mockito.when(proxyTblDao.queryByDcId(Mockito.anyLong(), Mockito.anyInt())).thenReturn(getProxyTbls());

        List<String> result = proxyService.getProxyUris("dc");
        Assert.assertEquals(result.size(), getProxyTbls().size());
    }

    @Test
    public void testGetProxyUri() throws Exception {
        Mockito.when(consoleConfig.getDcsInSameRegion(Mockito.anyString())).thenReturn(Sets.newHashSet("dc"));
        Mockito.when(dcTblDao.queryByDcName(Mockito.anyString())).thenReturn(getDcTbls().get(0));
        Mockito.when(proxyTblDao.queryByDcId(Mockito.anyLong(), Mockito.anyString(), Mockito.anyString())).thenReturn(getProxyTbls());

        List<String> res = proxyService.getProxyUris("dc", true);
        Assert.assertEquals(res.size(), getProxyTbls().size());
    }

    @Test
    public void testGetRelayProxyUri() throws Exception {
        Mockito.when(proxyTblDao.queryByPrefix(Mockito.anyString(), Mockito.anyString())).thenReturn(getProxyTbls());
        List<String> relayProxyUris = proxyService.getRelayProxyUris();
        Assert.assertEquals(relayProxyUris.size(), getProxyTbls().size());
    }

    @Test
    public void inputProxy() throws Exception {
        Mockito.when(dcTblDao.queryByDcName(Mockito.anyString())).thenReturn(getDcTbls().get(0));
        Mockito.doNothing().when(proxyTblDao).upsert(Mockito.any());
        proxyService.inputProxy("dc", "ip");
        Mockito.verify(proxyTblDao, Mockito.times(2)).upsert(Mockito.any());
    }


}
